from __future__ import annotations

from pathlib import Path
import hashlib
import json
import tempfile
import unittest

from identity_namespace import NamespaceContext, build_namespace_context
from migration_coordinator import MigrationCoordinator
from migration_dual_write import MigrationDualWriteProducer
from migration_outbox import MigrationOutbox
from migration_replay import MigrationReplayWorker
from relationship_account_store import RelationshipAccountStore
from relationship_event_policy import build_group_interaction_proof
from unified_person_registry import UnifiedPersonRegistry


POLICY = "req041-v1"


def _identity(subject: str = "10001") -> dict[str, str]:
    return {
        "companion_instance_id": "astrbot_plugin_private_companion",
        "bot_account_id": "onebot:bot-1",
        "adapter_instance_id": "onebot:default",
        "subject_namespace": "onebot:user",
        "platform_subject_id": subject,
    }


class MigrationReplayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name)
        source = self.data_dir / "companions.json"
        source.write_text('{"users":{}}', encoding="utf-8")
        self.coordinator = MigrationCoordinator(self.data_dir)
        status = self.coordinator.start_or_resume(
            source_files=[source], policy_version=POLICY,
            source_schema_version="legacy-effective", target_schema_version="req041-v1",
            companion_version="6.1.1", memory_version="1.7.2", reserve_bytes=0,
        )
        self.coordinator.capture_compatibility({})
        self.coordinator.transition("S3", checkpoint="outbox_active")
        self.coordinator.transition("S4", checkpoint="backfill_active")
        self.epoch = status["migration_epoch"]
        self.outbox = MigrationOutbox(self.data_dir / "outbox.db")
        self.outbox.begin_epoch(self.epoch, policy_version=POLICY)
        self.registry_data: dict = {}
        self.registry = UnifiedPersonRegistry(self.registry_data)
        created = self.registry.create_or_link(_identity(), operation_id="fixture-create")
        self.person_id = created["person_id"]
        self.coordinator.register_identity(self.person_id, assurance="verified")
        self.context = build_namespace_context(self.registry.formal_namespace_for_person(
            self.person_id, policy_version=POLICY, migration_epoch=self.epoch,
            purpose="relationship_write",
        )["context"])
        assert self.context is not None
        self.relationships = RelationshipAccountStore(
            self.data_dir / "relationships.db", active_migration_epoch=self.epoch,
            clock=lambda: 1_786_291_200.0,
        )
        self.relationships.create_account(
            self.context, operation_id="backfill", actor="migration",
            relationship_role="friend", relationship_mode="normal", score=10,
            positive_stage_cap_key="close", legacy_snapshot=True,
        )
        self.producer = MigrationDualWriteProducer(
            outbox=self.outbox, coordinator=self.coordinator,
            migration_epoch=self.epoch, policy_version=POLICY,
        )
        self.worker = MigrationReplayWorker(
            outbox=self.outbox, coordinator=self.coordinator,
            relationship_store=self.relationships, registry=self.registry,
            migration_epoch=self.epoch, policy_version=POLICY,
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _user(self, score: int = 12) -> dict:
        return {
            "user_id": "10001", "unified_person_id": self.person_id,
            "relationship_role": "friend", "relationship_mode": "normal",
            "relationship_score": score, "relationship_positive_stage_cap_key": "close",
            "relationship_daily_totals": {"day": "2026-08-10", "positive": 2, "negative": 0},
            "relationship_last_effective_at": 1_700_000_000,
        }

    def test_relationship_event_replays_exactly_and_is_restart_idempotent(self) -> None:
        self.producer.emit_relationship(
            registry=self.registry, user=self._user(), requested_delta=4,
            reason_code="inbound", source_revision=1,
            result={
                "changed": True, "delta": 2,
                "entry": {"event_key": "a" * 24, "score_before": 10, "score_after": 12},
            },
        )
        first = self.worker.run_batch()
        second = self.worker.run_batch()
        account = self.relationships.account(self.context)
        self.assertEqual(("ok", 1), (first["status"], first["count"]))
        self.assertEqual(("ok", 0), (second["status"], second["count"]))
        self.assertEqual(12, account["relationship_score"])
        self.assertEqual(1, self.outbox.applied_revision(f"relationship:{self.person_id}", self.epoch))
        identity = self.coordinator.identity_status(self.person_id)
        self.assertEqual("reconciling", identity["state"])
        self.assertEqual(2, identity["stable_cycles"])

    def test_pre_upgrade_private_event_hash_remains_replayable(self) -> None:
        proof = {
            "event_key": "z" * 24,
            "reason_code": "inbound",
            "applied_delta": 2,
            "score_before": 10,
            "score_after": 12,
        }
        digest = hashlib.sha256(json.dumps(
            proof, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")).hexdigest()
        self.outbox.enqueue_next(
            stream_key=f"relationship:{self.person_id}", event_id="old-private-event",
            namespace=self.context, migration_epoch=self.epoch, policy_version=POLICY,
            payload={
                "operation": "relationship_legacy_event", "identity_ref": self.person_id,
                **proof, "requested_delta": 4,
                "relationship_role": "friend", "relationship_mode": "normal",
                "positive_stage_cap_key": "close",
                "daily_totals": {"day": "2026-08-10", "positive": 2, "negative": 0},
                "last_effective_at": 1_700_000_000,
                "legacy_event_hash": digest,
            },
        )
        result = self.worker.run_batch()
        self.assertEqual(("ok", 1), (result["status"], result["count"]))
        self.assertEqual(12, self.relationships.account(self.context)["relationship_score"])

    def test_group_event_replay_requires_matching_admission_receipt(self) -> None:
        group = NamespaceContext(
            kind="group_member", persona_id=self.context.persona_id,
            identity_id=self.person_id, group_id="group@opaque-a",
            assurance=self.context.assurance, profile_status=self.context.profile_status,
            policy_version=POLICY, migration_epoch=self.epoch,
        )
        admission_event = "req041-group-affinity-proof"
        proof = build_group_interaction_proof(
            group, event_id=admission_event, directed_by="at_bot",
            inbound=True, human_sender=True, bot_reply_succeeded=True,
            forwarded=False, echo=False, historical=False,
        )
        admission = self.relationships.admit_group_event(
            group, event_id=admission_event, delta=4,
            allow_group_affinity=True, group_interaction_proof=proof,
        )
        self.assertEqual(1, admission.admitted_delta)
        user = self._user(score=11)
        user["relationship_daily_totals"] = {
            "day": "2026-08-10", "positive": 1, "negative": 0,
        }
        self.producer.emit_relationship(
            registry=self.registry, user=user, requested_delta=1,
            reason_code="direct_group_interaction", source_revision=1,
            group_admission_event_id=admission_event,
            result={
                "changed": True, "delta": 1,
                "entry": {"event_key": "g" * 24, "score_before": 10, "score_after": 11},
            },
        )
        result = self.worker.run_batch()
        self.assertEqual(("ok", 1), (result["status"], result["count"]))
        self.assertEqual(11, self.relationships.account(self.context)["relationship_score"])

    def test_group_event_without_admission_is_rejected_before_enqueue(self) -> None:
        user = self._user(score=11)
        with self.assertRaisesRegex(Exception, "dual_write_group_admission_missing"):
            self.producer.emit_relationship(
                registry=self.registry, user=user, requested_delta=1,
                reason_code="direct_group_interaction", source_revision=1,
                result={
                    "changed": True, "delta": 1,
                    "entry": {"event_key": "h" * 24, "score_before": 10, "score_after": 11},
                },
            )

    def test_late_identity_snapshot_creates_missing_account_and_replays_after_crash(self) -> None:
        created = self.registry.create_or_link(_identity("late-user"), operation_id="late-create")
        person_id = created["person_id"]
        self.coordinator.register_identity(person_id, assurance="verified")
        emitted = self.producer.emit_relationship_snapshot(
            registry=self.registry,
            user={
                "user_id": "late-user", "unified_person_id": person_id,
                "relationship_role": "owner", "relationship_mode": "normal",
                "relationship_score": 87, "relationship_positive_stage_cap_key": "deeply_bonded",
                "relationship_daily_totals": {"day": "", "positive": 0, "negative": 0},
                "relationship_last_effective_at": 0,
            },
            reason_code="migration_gap_recovery",
            source_revision=1,
        )
        self.assertEqual("enqueued", emitted["status"])
        item = next(
            pending for pending in self.outbox.pending(self.epoch)
            if pending.stream_key == f"relationship:{person_id}"
        )
        context, payload = self.worker._validate_envelope(item)
        self.worker._apply_relationship(item, context, payload)
        self.assertEqual(87, self.relationships.account(context)["relationship_score"])

        replayed = self.worker.run_batch()
        self.assertEqual("ok", replayed["status"])
        self.assertEqual(1, replayed["count"])
        account = self.relationships.account(context)
        self.assertEqual("owner", account["relationship_role"])
        self.assertEqual("normal", account["relationship_mode"])
        self.assertTrue(account["legacy_snapshot"])
        self.assertEqual(1, account["revision"])

    def test_event_arriving_before_any_snapshot_bootstraps_the_account(self) -> None:
        """A legacy event may precede its snapshot; it must still replay.

        The legacy ledger was written incrementally, so an event can reach the
        outbox while no account exists yet. The payload carries the complete
        "before" state, so the worker bootstraps the account from it and then
        replays the event instead of pausing the epoch forever with
        ``relationship_account_missing``.
        """
        # Drop the account seeded by setUp to reproduce the real ordering.
        self.relationships = RelationshipAccountStore(
            self.data_dir / "relationships-empty.db",
            active_migration_epoch=self.epoch,
            clock=lambda: 1_786_291_200.0,
        )
        producer = MigrationDualWriteProducer(
            outbox=self.outbox, coordinator=self.coordinator,
            migration_epoch=self.epoch, policy_version=POLICY,
        )
        producer.emit_relationship(
            registry=self.registry, user=self._user(), requested_delta=2,
            reason_code="fast_inbound", source_revision=1,
            result={
                "changed": True, "delta": 2,
                "entry": {"event_key": "b" * 24, "score_before": 10, "score_after": 12},
            },
        )
        worker = MigrationReplayWorker(
            outbox=self.outbox, coordinator=self.coordinator,
            relationship_store=self.relationships, registry=self.registry,
            migration_epoch=self.epoch, policy_version=POLICY,
        )

        result = worker.run_batch()

        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["count"])
        account = self.relationships.account(self.context)
        self.assertEqual(12, account["relationship_score"])
        # The alias must also be canonicalized in the account ledger.
        self.assertEqual("inbound", account["relationship_ledger"][-1]["reason_code"])

    def test_bad_event_proof_pauses_epoch_without_changing_target(self) -> None:
        self.outbox.enqueue_next(
            stream_key=f"relationship:{self.person_id}", event_id="bad-proof",
            namespace=self.context, migration_epoch=self.epoch, policy_version=POLICY,
            payload={
                "operation": "relationship_legacy_event", "identity_ref": self.person_id,
                "event_key": "b" * 24, "reason_code": "inbound", "requested_delta": 2,
                "applied_delta": 2, "score_before": 10, "score_after": 12,
                "relationship_role": "friend", "relationship_mode": "normal",
                "positive_stage_cap_key": "close",
                "daily_totals": {"day": "2026-08-10", "positive": 2, "negative": 0},
                "last_effective_at": 1_700_000_000, "legacy_event_hash": "0" * 64,
            },
        )
        result = self.worker.run_batch()
        self.assertEqual("paused", result["status"])
        self.assertEqual(10, self.relationships.account(self.context)["relationship_score"])
        self.assertEqual("paused", self.coordinator.status()["state"])
        self.assertEqual("failed", self.outbox.pending(self.epoch)[0].state)

    def test_identity_link_then_unlink_accepts_superseded_link_and_requires_tombstone(self) -> None:
        secondary = _identity("secondary")
        linked = self.registry.link_identity(self.person_id, secondary, operation_id="link")
        self.producer.emit_identity_change(
            registry=self.registry, result=linked, action="link", operation_id="link",
        )
        unlinked = self.registry.unlink_identity(
            self.person_id, secondary, operation_id="unlink", dry_run=False,
        )
        self.producer.emit_identity_change(
            registry=self.registry, result=unlinked, action="unlink", operation_id="unlink",
        )
        result = self.worker.run_batch()
        self.assertEqual(("ok", 2), (result["status"], result["count"]))
        self.assertEqual(2, self.outbox.applied_revision(f"identity:{self.person_id}", self.epoch))
        self.assertEqual(1, self.coordinator.identity_status(self.person_id)["stable_cycles"])

    def test_reconciliation_mismatch_records_state_and_pauses_cutover(self) -> None:
        self.producer.emit_relationship(
            registry=self.registry, user=self._user(), requested_delta=4,
            reason_code="inbound", source_revision=1,
            result={
                "changed": True, "delta": 2,
                "entry": {"event_key": "d" * 24, "score_before": 10, "score_after": 12},
            },
        )
        item = self.outbox.pending(self.epoch)[0]
        self.worker.apply_one(item)
        self.relationships.configure_account(
            self.context, operation_id="external-drift", actor="administrator",
            expected_revision=2, score=13,
        )
        result = self.worker.run_batch()
        identity = self.coordinator.identity_status(self.person_id)
        self.assertEqual("paused", result["status"])
        self.assertEqual("migration_reconcile_mismatch", result["error_code"])
        self.assertNotEqual(identity["source_hash"], identity["target_hash"])
        self.assertEqual(0, identity["stable_cycles"])

    def test_stream_revision_gap_fails_before_target_write(self) -> None:
        self.producer.emit_relationship(
            registry=self.registry, user=self._user(), requested_delta=4,
            reason_code="inbound", source_revision=1,
            result={
                "changed": True, "delta": 2,
                "entry": {"event_key": "c" * 24, "score_before": 10, "score_after": 12},
            },
        )
        item = self.outbox.pending(self.epoch)[0]
        object.__setattr__(item, "source_revision", 2)
        with self.assertRaisesRegex(Exception, "migration_replay_revision_gap"):
            self.worker.apply_one(item)
        self.assertEqual(10, self.relationships.account(self.context)["relationship_score"])

    def test_uncaptured_identity_write_after_baseline_is_detected(self) -> None:
        projection = self.registry.read_projection(self.person_id)
        checkpoint = self.registry.identity_projection_checkpoint(self.person_id)
        assert projection is not None
        self.outbox.enqueue_next(
            stream_key=f"identity:{self.person_id}", event_id="identity-baseline",
            namespace=self.context, migration_epoch=self.epoch, policy_version=POLICY,
            payload={
                "operation": "identity_baseline", "identity_ref": self.person_id,
                "identity_key_ref": projection["resolved_identity_key"],
                "identity_assurance": "verified", "profile_status": "active",
                "projection_revision": checkpoint["projection_revision"],
                "projection_checkpoint_hash": checkpoint["checkpoint_hash"],
            },
        )
        self.assertEqual("ok", self.worker.run_batch()["status"])
        self.registry.link_identity(
            self.person_id, _identity("uncaptured-secondary"), operation_id="uncaptured-link",
        )
        result = self.worker.run_batch()
        self.assertEqual("paused", result["status"])
        self.assertEqual("migration_reconcile_mismatch", result["error_code"])
        self.assertEqual(0, self.coordinator.identity_status(self.person_id)["stable_cycles"])

    def test_uncaptured_legacy_relationship_write_is_detected(self) -> None:
        live_state = {
            "relationship_role": "friend", "relationship_mode": "normal",
            "relationship_score": 12, "positive_stage_cap_key": "close",
            "daily_totals": {"day": "2026-08-10", "positive": 2, "negative": 0},
            "last_effective_at": 1_700_000_000.0,
        }
        worker = MigrationReplayWorker(
            outbox=self.outbox, coordinator=self.coordinator,
            relationship_store=self.relationships, registry=self.registry,
            legacy_relationship_resolver=lambda _person_id: dict(live_state),
            migration_epoch=self.epoch, policy_version=POLICY,
        )
        self.producer.emit_relationship(
            registry=self.registry, user=self._user(), requested_delta=4,
            reason_code="inbound", source_revision=1,
            result={
                "changed": True, "delta": 2,
                "entry": {"event_key": "e" * 24, "score_before": 10, "score_after": 12},
            },
        )
        self.assertEqual("ok", worker.run_batch()["status"])
        live_state["relationship_score"] = 13
        result = worker.run_batch()
        self.assertEqual("paused", result["status"])
        self.assertEqual("migration_reconcile_mismatch", result["error_code"])

    def test_gap_recovery_rebuilds_uncaptured_relationship_snapshot(self) -> None:
        live_state = {
            "relationship_role": "friend", "relationship_mode": "normal",
            "relationship_score": 10, "positive_stage_cap_key": "close",
            "daily_totals": {"day": "", "positive": 0, "negative": 0},
            "last_effective_at": 0.0,
        }
        worker = MigrationReplayWorker(
            outbox=self.outbox, coordinator=self.coordinator,
            relationship_store=self.relationships, registry=self.registry,
            legacy_relationship_resolver=lambda _person_id: dict(live_state),
            enable_gap_recovery=True,
            migration_epoch=self.epoch, policy_version=POLICY,
        )
        baseline = worker.run_batch()
        self.assertEqual("ok", baseline["status"])
        self.assertGreaterEqual(baseline["recovered"], 2)
        live_state["relationship_score"] = 13
        recovered = worker.run_batch()
        self.assertEqual("ok", recovered["status"])
        self.assertEqual(1, recovered["recovered"])
        self.assertEqual(13, self.relationships.account(self.context)["relationship_score"])
        self.assertEqual(2, self.outbox.stream_revision(f"relationship:{self.person_id}", self.epoch))
        live_state["relationship_score"] = 10
        repeated_state = worker.run_batch()
        self.assertEqual("ok", repeated_state["status"])
        self.assertEqual(1, repeated_state["recovered"])
        self.assertEqual(10, self.relationships.account(self.context)["relationship_score"])
        self.assertEqual(3, self.outbox.stream_revision(f"relationship:{self.person_id}", self.epoch))

    def test_gap_recovery_captures_uncaptured_link_and_unlink_tombstone(self) -> None:
        worker = MigrationReplayWorker(
            outbox=self.outbox, coordinator=self.coordinator,
            relationship_store=self.relationships, registry=self.registry,
            enable_gap_recovery=True,
            migration_epoch=self.epoch, policy_version=POLICY,
        )
        self.assertEqual("ok", worker.run_batch()["status"])
        secondary = _identity("gap-secondary")
        linked = self.registry.link_identity(
            self.person_id, secondary, operation_id="gap-link-without-producer",
        )
        link_recovery = worker.run_batch()
        self.assertEqual("ok", link_recovery["status"])
        self.assertEqual(1, link_recovery["recovered"])
        detached = self.registry.unlink_identity(
            self.person_id, secondary, operation_id="gap-unlink-without-producer", dry_run=False,
        )
        unlink_recovery = worker.run_batch()
        tombstone = self.outbox.tombstone(
            f"identity-link:{detached['identity_key']}", self.epoch
        )
        self.assertTrue(linked["changed"])
        self.assertTrue(detached["changed"])
        self.assertEqual("ok", unlink_recovery["status"])
        self.assertEqual(1, unlink_recovery["recovered"])
        self.assertEqual("identity_recovery_unlink", tombstone["reason_code"])


if __name__ == "__main__":
    unittest.main()
