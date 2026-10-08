"""REQ-041 ordered, idempotent Shadow target replay and verification."""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

try:
    from .identity_namespace import build_namespace_context
    from .migration_coordinator import MigrationCoordinator
    from .migration_outbox import MigrationOutbox, OutboxItem
    from .relationship_account_store import RelationshipAccountStore, RelationshipNotFound
    from .unified_person_registry import UnifiedPersonRegistry
except ImportError:  # pragma: no cover - direct-module test compatibility
    from identity_namespace import build_namespace_context
    from migration_coordinator import MigrationCoordinator
    from migration_outbox import MigrationOutbox, OutboxItem
    from relationship_account_store import RelationshipAccountStore, RelationshipNotFound
    from unified_person_registry import UnifiedPersonRegistry


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


class MigrationReplayError(RuntimeError):
    pass


class MigrationReplayWorker:
    """Apply one epoch in source order and stop the cutover on any mismatch."""

    def __init__(
        self,
        *,
        outbox: MigrationOutbox,
        coordinator: MigrationCoordinator,
        relationship_store: RelationshipAccountStore,
        registry: UnifiedPersonRegistry,
        registry_resolver: Any = None,
        legacy_relationship_resolver: Any = None,
        legacy_pending_resolver: Any = None,
        enable_gap_recovery: bool = False,
        migration_epoch: str,
        policy_version: str,
        observability: Any = None,
    ) -> None:
        self.outbox = outbox
        self.coordinator = coordinator
        self.relationship_store = relationship_store
        self.registry = registry
        self.registry_resolver = registry_resolver if callable(registry_resolver) else None
        self.legacy_relationship_resolver = (
            legacy_relationship_resolver if callable(legacy_relationship_resolver) else None
        )
        self.legacy_pending_resolver = (
            legacy_pending_resolver if callable(legacy_pending_resolver) else None
        )
        self._last_resolved_pending = 0
        self.enable_gap_recovery = bool(enable_gap_recovery)
        self.migration_epoch = str(migration_epoch or "").strip()
        self.policy_version = str(policy_version or "").strip()
        self.observability = observability
        if not self.migration_epoch or not self.policy_version:
            raise MigrationReplayError("migration_replay_contract_invalid")

    def _registry_for_person(self, person_id: str) -> UnifiedPersonRegistry:
        if self.registry_resolver is None:
            return self.registry
        candidate = self.registry_resolver(person_id)
        if not isinstance(candidate, UnifiedPersonRegistry):
            raise MigrationReplayError("migration_replay_registry_missing")
        return candidate

    @staticmethod
    def _relationship_state(account: dict[str, Any]) -> dict[str, Any]:
        return {
            "relationship_role": account.get("relationship_role"),
            "relationship_mode": account.get("relationship_mode"),
            "relationship_score": account.get("relationship_score"),
            "positive_stage_cap_key": account.get("relationship_positive_stage_cap_key"),
            "daily_totals": account.get("relationship_daily_totals"),
            "last_effective_at": account.get("relationship_last_effective_at"),
        }

    def _validate_envelope(self, item: OutboxItem) -> tuple[Any, dict[str, Any]]:
        if item.migration_epoch != self.migration_epoch or item.policy_version != self.policy_version:
            raise MigrationReplayError("migration_replay_epoch_stale")
        context = build_namespace_context(item.namespace)
        payload = item.payload
        if context is None or context.errors() or not isinstance(payload, dict):
            raise MigrationReplayError("migration_replay_envelope_invalid")
        if context.migration_epoch != self.migration_epoch or context.policy_version != self.policy_version:
            raise MigrationReplayError("migration_replay_namespace_stale")
        if payload.get("identity_ref") != context.identity_id:
            raise MigrationReplayError("migration_replay_identity_mismatch")
        expected_stream = (
            f"relationship:{context.identity_id}"
            if str(payload.get("operation") or "").startswith("relationship_")
            else f"identity:{context.identity_id}"
        )
        if item.stream_key != expected_stream:
            raise MigrationReplayError("migration_replay_stream_mismatch")
        return context, payload

    def _apply_relationship(self, item: OutboxItem, context: Any, payload: dict[str, Any]) -> int:
        operation = payload.get("operation")
        common = {
            "relationship_role": payload.get("relationship_role"),
            "relationship_mode": payload.get("relationship_mode"),
            "positive_stage_cap_key": payload.get("positive_stage_cap_key"),
            "daily_totals": payload.get("daily_totals"),
            "last_effective_at": payload.get("last_effective_at"),
        }
        if operation == "relationship_legacy_event":
            proof = {
                "event_key": payload.get("event_key"),
                "reason_code": payload.get("reason_code"),
                "applied_delta": payload.get("applied_delta"),
                "score_before": payload.get("score_before"),
                "score_after": payload.get("score_after"),
            }
            if "group_admission_event_ref" in payload:
                proof["group_admission_event_ref"] = payload.get("group_admission_event_ref") or ""
            if payload.get("legacy_event_hash") != _digest(proof):
                raise MigrationReplayError("migration_replay_event_proof_mismatch")
            if payload.get("reason_code") == "direct_group_interaction":
                admission_ref = str(payload.get("group_admission_event_ref") or "").strip()
                admission = self.relationship_store.group_admission(
                    context, event_id=admission_ref,
                ) if admission_ref else None
                if (
                    admission is None
                    or admission.identity_id != context.identity_id
                    or admission.admitted_delta != payload.get("applied_delta")
                ):
                    raise MigrationReplayError("migration_replay_group_admission_invalid")
            try:
                result = self.relationship_store.replay_legacy_event(
                    context,
                    event_id=item.event_id,
                    reason_code=payload.get("reason_code"),
                    requested_delta=payload.get("requested_delta"),
                    applied_delta=payload.get("applied_delta"),
                    score_before=payload.get("score_before"),
                    score_after=payload.get("score_after"),
                    **common,
                )
            except RelationshipNotFound:
                # A legacy event can reach the queue before its snapshot when the
                # legacy ledger was written incrementally. The event payload
                # carries the complete "before" account state (role, mode,
                # score, daily totals, last effective time), so bootstrap the
                # account from it and retry. This is not a guess: the replay
                # precondition still has to match score_before exactly.
                self.relationship_store.create_account(
                    context,
                    operation_id=f"{item.event_id}-bootstrap",
                    actor="migration",
                    relationship_role=common["relationship_role"],
                    relationship_mode=common["relationship_mode"],
                    score=payload.get("score_before"),
                    positive_stage_cap_key=common["positive_stage_cap_key"],
                    daily_totals=common["daily_totals"],
                    last_effective_at=common["last_effective_at"],
                )
                result = self.relationship_store.replay_legacy_event(
                    context,
                    event_id=item.event_id,
                    reason_code=payload.get("reason_code"),
                    requested_delta=payload.get("requested_delta"),
                    applied_delta=payload.get("applied_delta"),
                    score_before=payload.get("score_before"),
                    score_after=payload.get("score_after"),
                    **common,
                )
            if result.applied_delta != payload.get("applied_delta") or result.score != payload.get("score_after"):
                raise MigrationReplayError("migration_replay_event_result_mismatch")
        elif operation == "relationship_legacy_snapshot":
            expected = {
                "relationship_role": common["relationship_role"],
                "relationship_mode": common["relationship_mode"],
                "relationship_score": payload.get("relationship_score"),
                "positive_stage_cap_key": common["positive_stage_cap_key"],
                "daily_totals": common["daily_totals"],
                "last_effective_at": common["last_effective_at"],
            }
            if payload.get("snapshot_hash") != _digest(expected):
                raise MigrationReplayError("migration_replay_snapshot_proof_mismatch")
            try:
                existing = self.relationship_store.account(context)
            except RelationshipNotFound:
                self.relationship_store.create_account(
                    context,
                    operation_id=item.event_id,
                    actor="migration",
                    score=payload.get("relationship_score"),
                    legacy_snapshot=True,
                    **common,
                )
            else:
                if self._relationship_state(existing) != expected or not existing.get("legacy_snapshot"):
                    self.relationship_store.replay_legacy_snapshot(
                        context,
                        operation_id=item.event_id,
                        score=payload.get("relationship_score"),
                        **common,
                    )
        else:
            raise MigrationReplayError("migration_replay_operation_unsupported")
        account = self.relationship_store.account(context)
        expected_state = {
            "relationship_role": common["relationship_role"],
            "relationship_mode": common["relationship_mode"],
            "relationship_score": payload.get("score_after", payload.get("relationship_score")),
            "positive_stage_cap_key": common["positive_stage_cap_key"],
            "daily_totals": common["daily_totals"],
            "last_effective_at": float(common["last_effective_at"]),
        }
        if self._relationship_state(account) != expected_state:
            raise MigrationReplayError("migration_replay_relationship_state_mismatch")
        return int(account["revision"])

    def _apply_identity(self, item: OutboxItem, context: Any, payload: dict[str, Any]) -> int:
        operation = payload.get("operation")
        if operation not in {
            "identity_baseline", "identity_create", "identity_link", "identity_unlink",
            "identity_recovery_snapshot", "identity_recovery_unlink",
        }:
            raise MigrationReplayError("migration_replay_operation_unsupported")
        identity_key = str(payload.get("identity_key_ref") or "")
        state = self._registry_for_person(context.identity_id).identity_link_state(
            context.identity_id, identity_key
        )
        if not state.get("ok"):
            raise MigrationReplayError(str(state.get("code") or "migration_replay_identity_missing"))
        if operation in {"identity_unlink", "identity_recovery_unlink"}:
            if state.get("state") != "detached":
                raise MigrationReplayError("migration_replay_unlink_not_detached")
            tombstone = self.outbox.tombstone(f"identity-link:{identity_key}", self.migration_epoch)
            expected_reason = "identity_unlink" if operation == "identity_unlink" else "identity_recovery_unlink"
            if (
                tombstone.get("reason_code") != expected_reason
                or int(tombstone.get("revision") or 0) < item.source_revision
            ):
                raise MigrationReplayError("migration_replay_unlink_tombstone_missing")
        elif state.get("state") not in {"active", "detached"}:
            raise MigrationReplayError("migration_replay_link_state_invalid")
        if state.get("profile_status") != payload.get("profile_status"):
            raise MigrationReplayError("migration_replay_profile_status_mismatch")
        target_revision = int(state.get("projection_revision") or 0)
        if target_revision < int(payload.get("projection_revision") or 0):
            raise MigrationReplayError("migration_replay_projection_revision_stale")
        return target_revision

    def _formal_context(self, person_id: str) -> Any:
        resolution = self._registry_for_person(person_id).formal_namespace_for_person(
            person_id, policy_version=self.policy_version,
            migration_epoch=self.migration_epoch, purpose="relationship_write",
        )
        context = build_namespace_context(resolution.get("context") if isinstance(resolution, dict) else None)
        if context is None or not resolution.get("ok") or context.errors():
            raise MigrationReplayError("migration_gap_identity_not_formal")
        return context

    def _recover_relationship_gap(self, person_id: str) -> int:
        if self.legacy_relationship_resolver is None:
            return 0
        stream = f"relationship:{person_id}"
        if self.outbox.backlog_for_stream(stream, self.migration_epoch):
            return 0
        live = self.legacy_relationship_resolver(person_id)
        if not isinstance(live, dict):
            return 0
        latest = self.outbox.latest_for_stream(stream, self.migration_epoch)
        prior = self._source_relationship_state(latest.payload) if latest is not None else None
        if prior == live:
            return 0
        context = build_namespace_context(latest.namespace) if latest is not None else self._formal_context(person_id)
        if context is None or context.errors():
            raise MigrationReplayError("migration_gap_relationship_namespace_invalid")
        state_hash = _digest(live)
        next_revision = self.outbox.stream_revision(stream, self.migration_epoch) + 1
        event_id = "req041-rel-recovery-" + hashlib.sha256(
            f"{self.migration_epoch}:{person_id}:{next_revision}:{state_hash}".encode("utf-8")
        ).hexdigest()[:40]
        emitted = self.outbox.enqueue_next(
            stream_key=stream, event_id=event_id, namespace=context,
            migration_epoch=self.migration_epoch, policy_version=self.policy_version,
            payload={
                "operation": "relationship_legacy_snapshot", "identity_ref": person_id,
                **live, "snapshot_hash": state_hash, "reason_code": "migration_gap_recovery",
                "legacy_event_ref": state_hash[:24],
            },
        )
        return 1 if emitted.get("status") == "enqueued" else 0

    def _recover_identity_gap(self, person_id: str) -> int:
        stream = f"identity:{person_id}"
        if self.outbox.backlog_for_stream(stream, self.migration_epoch):
            return 0
        registry = self._registry_for_person(person_id)
        state = registry.identity_recovery_state(person_id)
        if not state.get("ok"):
            raise MigrationReplayError(str(state.get("code") or "migration_gap_identity_invalid"))
        latest = self.outbox.latest_for_stream(stream, self.migration_epoch)
        missing_detached = [
            identity_key
            for identity_key in state.get("detached_identity_keys", [])
            if not self.outbox.tombstone(f"identity-link:{identity_key}", self.migration_epoch)
        ]
        if not missing_detached and latest is not None and (
            latest.payload.get("projection_revision") == state.get("projection_revision")
            and latest.payload.get("projection_checkpoint_hash") == state.get("checkpoint_hash")
        ):
            return 0
        context = build_namespace_context(latest.namespace) if latest is not None else self._formal_context(person_id)
        if context is None or context.errors():
            raise MigrationReplayError("migration_gap_identity_namespace_invalid")
        recovered = 0
        for identity_key in missing_detached:
            event_id = "req041-id-recovery-unlink-" + hashlib.sha256(
                f"{self.migration_epoch}:{person_id}:{identity_key}:{state['checkpoint_hash']}".encode("utf-8")
            ).hexdigest()[:32]
            emitted = self.outbox.enqueue_next_with_tombstone(
                stream_key=stream, event_id=event_id, namespace=context,
                migration_epoch=self.migration_epoch, policy_version=self.policy_version,
                payload={
                    "operation": "identity_recovery_unlink", "identity_ref": person_id,
                    "identity_key_ref": identity_key, "identity_assurance": context.assurance,
                    "profile_status": context.profile_status,
                    "projection_revision": state["projection_revision"],
                    "projection_checkpoint_hash": state["checkpoint_hash"],
                },
                tombstone_key=f"identity-link:{identity_key}",
                reason_code="identity_recovery_unlink",
            )
            recovered += int(emitted.get("status") == "enqueued")
        if recovered:
            return recovered
        projection = registry.read_projection(person_id) or {}
        identity_key = str(projection.get("resolved_identity_key") or "")
        if not identity_key:
            raise MigrationReplayError("migration_gap_identity_primary_missing")
        event_id = "req041-id-recovery-snapshot-" + hashlib.sha256(
            f"{self.migration_epoch}:{person_id}:{state['checkpoint_hash']}".encode("utf-8")
        ).hexdigest()[:32]
        emitted = self.outbox.enqueue_next(
            stream_key=stream, event_id=event_id, namespace=context,
            migration_epoch=self.migration_epoch, policy_version=self.policy_version,
            payload={
                "operation": "identity_recovery_snapshot", "identity_ref": person_id,
                "identity_key_ref": identity_key, "identity_assurance": context.assurance,
                "profile_status": context.profile_status,
                "projection_revision": state["projection_revision"],
                "projection_checkpoint_hash": state["checkpoint_hash"],
            },
        )
        return int(emitted.get("status") == "enqueued")

    def recover_gaps(self) -> int:
        if not self.enable_gap_recovery:
            return 0
        recovered = 0
        for person_id in self.coordinator.identity_ids():
            recovered += self._recover_identity_gap(person_id)
            recovered += self._recover_relationship_gap(person_id)
        return recovered

    def switch_ready_identities(self, *, required_stable_cycles: int = 2) -> list[str]:
        status = self.coordinator.status()
        if status.get("phase") not in {"S6", "S7", "S8", "S9"}:
            return []
        switched: list[str] = []
        for person_id in self.coordinator.ready_identity_ids(
            required_stable_cycles=required_stable_cycles
        ):
            result = self.coordinator.switch_identity_to_new_read(
                person_id, required_stable_cycles=required_stable_cycles
            )
            if result.get("read_generation") == "new":
                switched.append(person_id)
        return switched

    def apply_one(self, item: OutboxItem) -> dict[str, Any]:
        expected = self.outbox.applied_revision(item.stream_key, self.migration_epoch) + 1
        if item.source_revision != expected:
            raise MigrationReplayError("migration_replay_revision_gap")
        context, payload = self._validate_envelope(item)
        if str(payload.get("operation") or "").startswith("relationship_"):
            target_revision = self._apply_relationship(item, context, payload)
        else:
            target_revision = self._apply_identity(item, context, payload)
        self.outbox.mark_applied(item.event_id, self.migration_epoch, target_revision=target_revision)
        return {
            "event_id": item.event_id,
            "stream_key": item.stream_key,
            "source_revision": item.source_revision,
            "target_revision": target_revision,
            "status": "applied",
        }

    def _fail_closed(self, item: OutboxItem, error: Exception) -> None:
        code = str(error).split(":", 1)[0].strip() or "migration_replay_failed"
        if len(code) > 80 or not code.replace("_", "").isalnum():
            code = "migration_replay_failed"
        self.outbox.mark_failed(item.event_id, self.migration_epoch, error_code=code)
        identity_id = str(item.namespace.get("identity_id") or "") if isinstance(item.namespace, dict) else ""
        if identity_id:
            try:
                self.coordinator.rollback_identity(identity_id, reason_code=code)
            except Exception:
                pass
        self.coordinator.pause(code)
        try:
            self.outbox.set_epoch_state(self.migration_epoch, "degraded", checkpoint=code)
        except Exception:
            pass

    def _pause_reconciliation(self, error: Exception) -> str:
        code = str(error).split(":", 1)[0].strip() or "migration_reconcile_failed"
        if len(code) > 80 or not code.replace("_", "").isalnum():
            code = "migration_reconcile_failed"
        self.coordinator.pause(code)
        try:
            self.outbox.set_epoch_state(self.migration_epoch, "degraded", checkpoint=code)
        except Exception:
            pass
        return code

    @staticmethod
    def _source_relationship_state(payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "relationship_role": payload.get("relationship_role"),
            "relationship_mode": payload.get("relationship_mode"),
            "relationship_score": payload.get("score_after", payload.get("relationship_score")),
            "positive_stage_cap_key": payload.get("positive_stage_cap_key"),
            "daily_totals": payload.get("daily_totals"),
            "last_effective_at": float(payload.get("last_effective_at") or 0.0),
        }

    @staticmethod
    def _identity_projection_state(projection: dict[str, Any] | None) -> dict[str, Any]:
        value = projection if isinstance(projection, dict) else {}
        return {
            "person_id": value.get("person_id"),
            "resolved_identity_key": value.get("resolved_identity_key"),
            "identity_assurance": value.get("identity_assurance"),
            "profile_status": value.get("profile_status"),
            "projection_revision": value.get("projection_revision"),
        }

    def reconcile_all(self) -> list[dict[str, Any]]:
        """Reconcile one combined identity, relationship and backlog checkpoint."""
        self._last_resolved_pending = 0
        streams = self.outbox.stream_keys(self.migration_epoch)
        people = sorted(
            {stream.split(":", 1)[1] for stream in streams if ":" in stream}
            | set(self.coordinator.identity_ids())
        )
        results: list[dict[str, Any]] = []
        for person_id in people:
            identity_stream = f"identity:{person_id}"
            relationship_stream = f"relationship:{person_id}"
            source_revision = sum(
                self.outbox.stream_revision(stream, self.migration_epoch)
                for stream in (identity_stream, relationship_stream)
            )
            target_revision = sum(
                self.outbox.applied_revision(stream, self.migration_epoch)
                for stream in (identity_stream, relationship_stream)
            )
            backlog = sum(
                self.outbox.backlog_for_stream(stream, self.migration_epoch)
                for stream in (identity_stream, relationship_stream)
            )
            projection_state = self._identity_projection_state(
                self._registry_for_person(person_id).read_projection(person_id)
            )
            source_state: dict[str, Any] = {"identity": projection_state}
            target_state: dict[str, Any] = {"identity": projection_state}
            latest_identity = self.outbox.latest_for_stream(identity_stream, self.migration_epoch)
            if latest_identity is not None:
                checkpoint = self._registry_for_person(person_id).identity_projection_checkpoint(person_id)
                if not checkpoint.get("ok"):
                    raise MigrationReplayError("migration_reconcile_identity_checkpoint_invalid")
                source_state["identity_checkpoint"] = {
                    "projection_revision": latest_identity.payload.get("projection_revision"),
                    "checkpoint_hash": latest_identity.payload.get("projection_checkpoint_hash"),
                }
                target_state["identity_checkpoint"] = {
                    "projection_revision": checkpoint.get("projection_revision"),
                    "checkpoint_hash": checkpoint.get("checkpoint_hash"),
                }
            latest_relationship = self.outbox.latest_for_stream(relationship_stream, self.migration_epoch)
            if latest_relationship is not None:
                context = build_namespace_context(latest_relationship.namespace)
                if context is None or context.errors():
                    raise MigrationReplayError("migration_reconcile_namespace_invalid")
                payload_state = self._source_relationship_state(latest_relationship.payload)
                live_state = (
                    self.legacy_relationship_resolver(person_id)
                    if self.legacy_relationship_resolver is not None else payload_state
                )
                if not isinstance(live_state, dict):
                    raise MigrationReplayError("migration_reconcile_legacy_relationship_missing")
                source_state["relationship"] = live_state
                target_state["relationship"] = self._relationship_state(
                    self.relationship_store.account(context)
                )
            else:
                registry = self._registry_for_person(person_id)
                resolution = registry.formal_namespace_for_person(
                    person_id, policy_version=self.policy_version,
                    migration_epoch=self.migration_epoch, purpose="relationship_read",
                )
                context = build_namespace_context(resolution.get("context") if isinstance(resolution, dict) else None)
                if context is None or not resolution.get("ok") or context.errors():
                    continue
                try:
                    baseline = self._relationship_state(self.relationship_store.account(context))
                except RelationshipNotFound:
                    continue
                live_state = (
                    self.legacy_relationship_resolver(person_id)
                    if self.legacy_relationship_resolver is not None else baseline
                )
                if not isinstance(live_state, dict):
                    raise MigrationReplayError("migration_reconcile_legacy_relationship_missing")
                source_state["relationship"] = live_state
                target_state["relationship"] = baseline
            source_hash = _digest(source_state)
            target_hash = _digest(target_state)
            reconciled = self.coordinator.reconcile_identity(
                person_id,
                source_revision=source_revision,
                target_revision=target_revision,
                source_hash=source_hash,
                target_hash=target_hash,
                backlog=backlog,
            )
            results.append(reconciled)
            if backlog == 0 and (
                source_revision != target_revision or source_hash != target_hash
            ):
                raise MigrationReplayError("migration_reconcile_mismatch")
            if (
                self.legacy_pending_resolver is not None
                and backlog == 0
                and source_revision == target_revision
                and source_hash == target_hash
                and int(reconciled.get("stable_cycles") or 0) >= 2
            ):
                self._last_resolved_pending += max(
                    0, int(self.legacy_pending_resolver(person_id) or 0)
                )
        return results

    def run_batch(self, *, limit: int = 100) -> dict[str, Any]:
        started = time.perf_counter()
        applied: list[dict[str, Any]] = []
        try:
            recovered = self.recover_gaps()
        except Exception as exc:
            code = self._pause_reconciliation(exc)
            self._observe_batch(started, mismatch=True)
            return {"status": "paused", "applied": applied, "error_code": code}
        for item in self.outbox.pending(self.migration_epoch, limit=limit):
            try:
                applied.append(self.apply_one(item))
            except Exception as exc:
                self._fail_closed(item, exc)
                self._observe_batch(started, mismatch=True)
                return {"status": "paused", "applied": applied, "error_code": str(exc).split(":", 1)[0]}
        try:
            reconciled = self.reconcile_all()
        except Exception as exc:
            code = self._pause_reconciliation(exc)
            self._observe_batch(started, mismatch=True)
            return {"status": "paused", "applied": applied, "error_code": code}
        switched = self.switch_ready_identities()
        self._observe_batch(started, mismatch=False)
        return {
            "status": "ok", "applied": applied, "count": len(applied),
            "recovered": recovered, "resolved_pending": self._last_resolved_pending,
            "reconciled": reconciled, "switched": switched,
        }

    def _observe_batch(self, started: float, *, mismatch: bool) -> None:
        if self.observability is None:
            return
        self.observability.observe(
            "migration_replay", (time.perf_counter() - started) * 1000.0,
        )
        if mismatch:
            self.observability.increment("migration_mismatch")
        status = self.coordinator.status()
        pending = self.coordinator.pending_summary()
        backlog = sum(
            self.outbox.backlog_for_stream(stream, self.migration_epoch)
            for stream in self.outbox.stream_keys(self.migration_epoch)
        )
        self.observability.migration(
            state="paused" if mismatch else str(status.get("state") or "active"),
            phase=str(status.get("phase") or ""), backlog=backlog,
            pending=int(pending.get("total") or 0),
            mismatches=1 if mismatch else 0,
        )


__all__ = ["MigrationReplayError", "MigrationReplayWorker"]
