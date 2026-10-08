"""REQ-041 restart-safe automatic migration coordinator.

The coordinator never mutates legacy source files.  It owns only a control DB,
verified backups, per-identity read generations and request-chain leases.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sqlite3
import threading
import time
from typing import Any, Iterator, Sequence


PHASES = ("S0", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9")
STATES = frozenset({"active", "degraded", "paused", "replaying", "verified"})
IDENTITY_STATES = frozenset({"pending", "backfilling", "reconciling", "new_read", "legacy_read", "error"})
FORMAL_ASSURANCE = frozenset({"verified", "explicit_linked"})
COMPATIBILITY_KEYS = frozenset({
    "auto_profile_creation", "content_policy", "owner_policy", "private_access_policy",
    "proactive_policy", "relationship_policy", "tool_policy",
})
# Pause reasons that describe a *replay/apply* failure rather than an
# integrity problem with the migration source. These can legitimately clear on
# their own once the underlying code is fixed: the queue is replayed from the
# durable outbox, and replay is idempotent, so retrying is safe. Integrity
# reasons (backup/source changed) stay fail-closed and need a human.
RECOVERABLE_PAUSE_REASONS = frozenset({
    "relationship_legacy_event_invalid",
    "migration_replay_failed",
    "migration_replay_event_proof_mismatch",
    "migration_replay_event_result_mismatch",
    "migration_replay_identity_mismatch",
    "migration_replay_stream_mismatch",
    "migration_replay_group_admission_invalid",
    "migration_reconcile_failed",
})
SOURCE_INVENTORY_KEYS = frozenset({
    "schema", "source_schema_version", "fingerprint", "source_count", "formats",
    "store_version", "section_schema_versions", "all_have_unified_person",
    "all_have_persona_lifecycle", "section_count_min", "section_count_max",
})


class MigrationCoordinatorError(RuntimeError):
    pass


class MigrationPreflightError(MigrationCoordinatorError):
    pass


class MigrationStateConflict(MigrationCoordinatorError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _token(value: Any, limit: int = 128) -> str:
    if not isinstance(value, str):
        return ""
    result = value.strip()
    if not result or len(result) > limit or any(ord(char) < 32 for char in result):
        return ""
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _digest(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if re.fullmatch(r"[0-9a-f]{64}", text) else ""


def _is_sqlite_file(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            return handle.read(16) == b"SQLite format 3\x00"
    except OSError:
        return False


def _safe_snapshot(value: Any, depth: int = 0) -> Any:
    if depth > 3:
        raise MigrationCoordinatorError("compatibility_snapshot_invalid")
    if value is None or type(value) in {bool, int, float}:
        return value
    if isinstance(value, str):
        text = value.strip()
        if len(text) > 160 or "\n" in text or "\r" in text:
            raise MigrationCoordinatorError("compatibility_snapshot_invalid")
        return text
    if isinstance(value, list):
        if len(value) > 64:
            raise MigrationCoordinatorError("compatibility_snapshot_invalid")
        return [_safe_snapshot(item, depth + 1) for item in value]
    if isinstance(value, dict):
        if len(value) > 64:
            raise MigrationCoordinatorError("compatibility_snapshot_invalid")
        result: dict[str, Any] = {}
        for key, item in value.items():
            clean = _token(key, 80)
            if not clean or any(marker in clean.lower() for marker in ("secret", "token", "password", "credential", "raw")):
                raise MigrationCoordinatorError("compatibility_snapshot_forbidden")
            result[clean] = _safe_snapshot(item, depth + 1)
        return result
    raise MigrationCoordinatorError("compatibility_snapshot_invalid")


def _source_inventory(value: Any, *, source_schema_version: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict) or set(value) != SOURCE_INVENTORY_KEYS:
        raise MigrationPreflightError("migration_source_inventory_invalid")
    safe = _safe_snapshot(value)
    if (
        safe.get("schema") != "req041.source_inventory.v1"
        or safe.get("source_schema_version") != source_schema_version
        or not _digest(safe.get("fingerprint"))
        or source_schema_version != (
            f"companion-v{safe.get('store_version')}-{str(safe.get('fingerprint') or '')[:32]}"
        )
        or type(safe.get("source_count")) is not int
        or safe["source_count"] <= 0
        or type(safe.get("store_version")) is not int
        or type(safe.get("section_count_min")) is not int
        or type(safe.get("section_count_max")) is not int
        or safe["section_count_min"] < 3
        or safe["section_count_max"] < safe["section_count_min"]
        or type(safe.get("all_have_unified_person")) is not bool
        or type(safe.get("all_have_persona_lifecycle")) is not bool
    ):
        raise MigrationPreflightError("migration_source_inventory_invalid")
    formats = safe.get("formats")
    if (
        not isinstance(formats, dict)
        or set(formats) != {"json", "sqlite"}
        or any(type(formats[kind]) is not int or formats[kind] < 0 for kind in formats)
        or sum(formats.values()) != safe["source_count"]
    ):
        raise MigrationPreflightError("migration_source_inventory_invalid")
    section_versions = safe.get("section_schema_versions")
    if (
        not isinstance(section_versions, list)
        or any(type(item) is not int or item <= 0 for item in section_versions)
        or section_versions != sorted(set(section_versions))
    ):
        raise MigrationPreflightError("migration_source_inventory_invalid")
    return safe


def _source_inventory_contract(value: dict[str, Any]) -> dict[str, Any]:
    """Select only fields that cannot change through ordinary legacy saves."""
    return {
        key: value.get(key)
        for key in (
            "schema", "source_schema_version", "fingerprint", "source_count",
            "formats", "store_version", "section_schema_versions",
        )
    }


class MigrationCoordinator:
    def __init__(self, data_dir: str | Path, *, clock: Any = None) -> None:
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.data_dir / "req041_migration_control.db"
        self.backup_root = self.data_dir / "req041_backups"
        self._clock = clock if callable(clock) else time.time
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=15.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                if connection.in_transaction:
                    connection.execute("COMMIT")
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

    def _initialize(self) -> None:
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS migration_control (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    migration_epoch TEXT NOT NULL, policy_version TEXT NOT NULL,
                    phase TEXT NOT NULL, state TEXT NOT NULL, checkpoint TEXT NOT NULL,
                    source_schema_version TEXT NOT NULL, target_schema_version TEXT NOT NULL,
                    companion_version TEXT NOT NULL, memory_version TEXT NOT NULL,
                    backup_manifest TEXT NOT NULL, backup_manifest_hash TEXT NOT NULL,
                    compatibility_json TEXT NOT NULL, error_code TEXT NOT NULL,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS migration_identities (
                    identity_id TEXT PRIMARY KEY, assurance TEXT NOT NULL, state TEXT NOT NULL,
                    read_generation TEXT NOT NULL, dual_write INTEGER NOT NULL,
                    source_revision INTEGER NOT NULL, target_revision INTEGER NOT NULL,
                    source_hash TEXT NOT NULL, target_hash TEXT NOT NULL,
                    backlog INTEGER NOT NULL, stable_cycles INTEGER NOT NULL,
                    error_code TEXT NOT NULL, updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS migration_read_leases (
                    chain_id TEXT PRIMARY KEY, identity_id TEXT NOT NULL, read_generation TEXT NOT NULL,
                    migration_epoch TEXT NOT NULL, created_at REAL NOT NULL,
                    FOREIGN KEY(identity_id) REFERENCES migration_identities(identity_id)
                );
                CREATE TABLE IF NOT EXISTS migration_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, operation TEXT NOT NULL,
                    identity_hash TEXT NOT NULL, code TEXT NOT NULL, created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS migration_pending_records (
                    legacy_ref_hash TEXT PRIMARY KEY,
                    source_kind TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    state TEXT NOT NULL,
                    first_seen_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )

    def _source_paths(self, source_files: Sequence[str | Path]) -> list[Path]:
        result: list[Path] = []
        for value in source_files:
            candidate = Path(value)
            if not candidate.is_absolute():
                candidate = self.data_dir / candidate
            if candidate.is_symlink():
                raise MigrationPreflightError("migration_source_file_invalid")
            try:
                resolved = candidate.resolve(strict=True)
                resolved.relative_to(self.data_dir)
            except (OSError, ValueError) as exc:
                raise MigrationPreflightError("migration_source_path_invalid") from exc
            if (
                resolved.is_symlink() or not resolved.is_file() or self.backup_root in resolved.parents
                or resolved.name.startswith(self.path.name)
            ):
                raise MigrationPreflightError("migration_source_file_invalid")
            if not os.access(resolved, os.R_OK):
                raise MigrationPreflightError("migration_source_unreadable")
            if resolved not in result:
                result.append(resolved)
        if not result:
            raise MigrationPreflightError("migration_source_missing")
        return sorted(result)

    def preflight(self, source_files: Sequence[str | Path], *, reserve_bytes: int = 10 * 1024 * 1024) -> dict[str, Any]:
        sources = self._source_paths(source_files)
        total = sum(path.stat().st_size for path in sources)
        self.backup_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        free = shutil.disk_usage(self.data_dir).free
        required = total * 2 + max(0, int(reserve_bytes))
        if free < required:
            raise MigrationPreflightError("migration_space_insufficient")
        return {"source_count": len(sources), "source_bytes": total, "free_bytes": free, "required_bytes": required}

    def _new_epoch(self) -> str:
        stamp = datetime.fromtimestamp(float(self._clock()), timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"req041-{stamp}-{secrets.token_hex(6)}"

    def initialize_fresh_runtime(
        self,
        *,
        policy_version: str,
        target_schema_version: str,
        companion_version: str,
        memory_version: str,
    ) -> dict[str, Any]:
        """Create the durable scoped control plane for an install with no legacy source."""
        policy = _token(policy_version, 64)
        versions = [
            _token(target_schema_version, 64),
            _token(companion_version, 32),
            _token(memory_version, 32),
        ]
        if not policy or not all(versions):
            raise MigrationPreflightError("fresh_runtime_version_invalid")
        source_schema = "req041-fresh-v1"
        now = float(self._clock())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM migration_control WHERE singleton=1"
            ).fetchone()
            if row is None:
                connection.execute(
                    """INSERT INTO migration_control(
                       singleton,migration_epoch,policy_version,phase,state,checkpoint,
                       source_schema_version,target_schema_version,companion_version,memory_version,
                       backup_manifest,backup_manifest_hash,compatibility_json,error_code,created_at,updated_at)
                       VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        self._new_epoch(), policy, "S9", "active", "fresh_runtime_initialized",
                        source_schema, versions[0], versions[1], versions[2],
                        "", "", "{}", "", now, now,
                    ),
                )
            elif (
                row["source_schema_version"] != source_schema
                or row["policy_version"] != policy
                or row["target_schema_version"] != versions[0]
            ):
                raise MigrationStateConflict("fresh_runtime_contract_conflict")
        return self.status()

    def start_or_resume(
        self,
        *,
        source_files: Sequence[str | Path],
        policy_version: str,
        source_schema_version: str,
        target_schema_version: str,
        companion_version: str,
        memory_version: str,
        source_inventory: dict[str, Any] | None = None,
        reserve_bytes: int = 10 * 1024 * 1024,
    ) -> dict[str, Any]:
        policy = _token(policy_version, 64)
        versions = [
            _token(source_schema_version, 64), _token(target_schema_version, 64),
            _token(companion_version, 32), _token(memory_version, 32),
        ]
        if not policy or not all(versions):
            raise MigrationPreflightError("migration_version_invalid")
        inventory = _source_inventory(source_inventory, source_schema_version=versions[0])
        sources = self._source_paths(source_files)
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM migration_control WHERE singleton=1").fetchone()
            if row is None:
                self.preflight(sources, reserve_bytes=reserve_bytes)
                now = float(self._clock())
                epoch = self._new_epoch()
                connection.execute(
                    """INSERT INTO migration_control(
                       singleton,migration_epoch,policy_version,phase,state,checkpoint,
                       source_schema_version,target_schema_version,companion_version,memory_version,
                       backup_manifest,backup_manifest_hash,compatibility_json,error_code,created_at,updated_at)
                       VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (epoch, policy, "S0", "active", "", versions[0], versions[1], versions[2], versions[3],
                     "", "", "{}", "", now, now),
                )
            else:
                if (
                    row["policy_version"] != policy
                    or row["source_schema_version"] != versions[0]
                    or row["target_schema_version"] != versions[1]
                ):
                    raise MigrationStateConflict("migration_resume_contract_conflict")
                epoch = row["migration_epoch"]
        status = self.status()
        if status["phase"] == "S0" or not status["backup_manifest_hash"]:
            self._create_verified_backup(epoch, sources, versions=versions, source_inventory=inventory)
        else:
            if not self.verify_backup():
                self.pause("migration_backup_unverified")
                raise MigrationStateConflict("migration_backup_unverified")
            manifest_path = self.data_dir / status["backup_manifest"]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            expected_names = sorted(path.relative_to(self.data_dir).as_posix() for path in sources)
            observed_names = sorted(str(item.get("name") or "") for item in manifest.get("files", []))
            if expected_names != observed_names:
                self.pause("migration_source_set_changed")
                raise MigrationStateConflict("migration_source_set_changed")
            if inventory and _source_inventory_contract(manifest.get("source_inventory") or {}) != _source_inventory_contract(inventory):
                self.pause("migration_source_inventory_changed")
                raise MigrationStateConflict("migration_source_inventory_changed")
        return self.status()

    def _create_verified_backup(
        self,
        epoch: str,
        sources: Sequence[Path],
        *,
        versions: Sequence[str],
        source_inventory: dict[str, Any],
    ) -> None:
        destination = self.backup_root / epoch
        destination.mkdir(mode=0o700, parents=True, exist_ok=True)
        entries: list[dict[str, Any]] = []
        files_root = destination / "files"
        files_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        for source in sources:
            relative = source.relative_to(self.data_dir)
            target = files_root / relative
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.tmp")
            temporary.unlink(missing_ok=True)
            source_kind = "sqlite" if _is_sqlite_file(source) else "file"
            if source_kind == "sqlite":
                source_connection = sqlite3.connect(f"file:{source}?mode=ro", uri=True, timeout=15.0)
                target_connection = sqlite3.connect(str(temporary), timeout=15.0)
                try:
                    source_connection.backup(target_connection)
                    check = target_connection.execute("PRAGMA quick_check").fetchone()
                    if check is None or str(check[0]).lower() != "ok":
                        raise MigrationPreflightError("migration_sqlite_backup_invalid")
                finally:
                    target_connection.close()
                    source_connection.close()
                copied = _sha256(temporary)
            else:
                before = _sha256(source)
                shutil.copy2(source, temporary)
                copied = _sha256(temporary)
                after = _sha256(source)
                if before != copied or before != after:
                    temporary.unlink(missing_ok=True)
                    self.pause("migration_source_changed")
                    raise MigrationPreflightError("migration_source_changed")
            os.chmod(temporary, 0o600)
            os.replace(temporary, target)
            os.chmod(target, 0o400)
            entries.append({
                "name": relative.as_posix(), "kind": source_kind,
                "bytes": target.stat().st_size, "sha256": copied,
            })
        manifest = {
            "schema": "req041.backup_manifest.v2" if source_inventory else "req041.backup_manifest.v1",
            "migration_epoch": epoch,
            "source_schema_version": versions[0], "target_schema_version": versions[1],
            "companion_version": versions[2], "memory_version": versions[3], "files": entries,
        }
        if source_inventory:
            manifest["source_inventory"] = source_inventory
        encoded = _canonical(manifest)
        manifest_path = destination / "manifest.json"
        temporary_manifest = destination / ".manifest.json.tmp"
        temporary_manifest.write_text(encoded, encoding="utf-8")
        os.chmod(temporary_manifest, 0o600)
        os.replace(temporary_manifest, manifest_path)
        os.chmod(manifest_path, 0o400)
        manifest_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        with self._transaction() as connection:
            connection.execute(
                """UPDATE migration_control SET phase='S1',backup_manifest=?,backup_manifest_hash=?,
                   checkpoint='backup_verified',error_code='',updated_at=? WHERE singleton=1 AND migration_epoch=?""",
                (str(manifest_path.relative_to(self.data_dir)), manifest_hash, float(self._clock()), epoch),
            )

    def verify_backup(self) -> bool:
        status = self.status()
        relative = status.get("backup_manifest") or ""
        if not relative:
            return False
        try:
            manifest_path = (self.data_dir / relative).resolve(strict=True)
            manifest_path.relative_to(self.backup_root)
            encoded = manifest_path.read_text(encoding="utf-8")
            if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != status["backup_manifest_hash"]:
                return False
            manifest = json.loads(encoded)
            for item in manifest.get("files", []):
                path = manifest_path.parent / "files" / item["name"]
                if path.stat().st_size != item["bytes"] or _sha256(path) != item["sha256"]:
                    return False
            return True
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            return False

    def capture_compatibility(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(snapshot, dict) or not set(snapshot).issubset(COMPATIBILITY_KEYS):
            raise MigrationCoordinatorError("compatibility_snapshot_keys_invalid")
        safe = _safe_snapshot(snapshot)
        if not self.verify_backup():
            raise MigrationStateConflict("migration_backup_unverified")
        with self._transaction() as connection:
            row = connection.execute("SELECT phase,state FROM migration_control WHERE singleton=1").fetchone()
            if row is None or row["phase"] not in {"S1", "S2"} or row["state"] == "paused":
                raise MigrationStateConflict("compatibility_capture_denied")
            connection.execute(
                "UPDATE migration_control SET phase='S2',compatibility_json=?,checkpoint='compatibility_captured',updated_at=? WHERE singleton=1",
                (_canonical(safe), float(self._clock())),
            )
        return safe

    def transition(self, phase: str, *, checkpoint: str) -> dict[str, Any]:
        if phase not in PHASES or not _token(checkpoint, 256):
            raise MigrationCoordinatorError("migration_transition_invalid")
        if not self.verify_backup():
            raise MigrationStateConflict("migration_backup_unverified")
        with self._transaction() as connection:
            row = connection.execute("SELECT phase,state FROM migration_control WHERE singleton=1").fetchone()
            current_index = PHASES.index(row["phase"]) if row is not None else -1
            target_index = PHASES.index(phase)
            if (
                row is None or row["state"] == "paused" or target_index < current_index
                or target_index > current_index + 1
            ):
                raise MigrationStateConflict("migration_transition_denied")
            connection.execute(
                "UPDATE migration_control SET phase=?,checkpoint=?,updated_at=? WHERE singleton=1",
                (phase, checkpoint, float(self._clock())),
            )
        return self.status()

    def register_identity(self, identity_id: str, *, assurance: str) -> dict[str, Any]:
        identity = _token(identity_id)
        if not identity or assurance not in {"unverified", "observed", *FORMAL_ASSURANCE}:
            raise MigrationCoordinatorError("migration_identity_invalid")
        state = "pending" if assurance not in FORMAL_ASSURANCE else "legacy_read"
        now = float(self._clock())
        with self._transaction() as connection:
            connection.execute(
                """INSERT INTO migration_identities VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(identity_id) DO UPDATE SET assurance=excluded.assurance,
                   state=CASE
                       WHEN excluded.assurance IN ('verified','explicit_linked') AND migration_identities.state='new_read'
                           THEN 'new_read'
                       ELSE excluded.state END,
                   read_generation=CASE
                       WHEN excluded.assurance IN ('verified','explicit_linked') THEN migration_identities.read_generation
                       ELSE 'legacy' END,
                   updated_at=excluded.updated_at""",
                (identity, assurance, state, "legacy", 1, 0, 0, "", "", 0, 0, "", now),
            )
        return self.identity_status(identity)

    def record_pending(self, legacy_ref_hash: str, *, source_kind: str, reason_code: str) -> dict[str, Any]:
        """Persist an opaque migration exception without retaining a raw user key."""
        reference = _digest(legacy_ref_hash)
        source = _token(source_kind, 40)
        reason = _token(reason_code, 80)
        if not reference or not source or not reason:
            raise MigrationCoordinatorError("migration_pending_invalid")
        now = float(self._clock())
        with self._transaction() as connection:
            prior = connection.execute(
                "SELECT source_kind,reason_code,state FROM migration_pending_records WHERE legacy_ref_hash=?",
                (reference,),
            ).fetchone()
            connection.execute(
                """INSERT INTO migration_pending_records(
                       legacy_ref_hash,source_kind,reason_code,state,first_seen_at,updated_at)
                   VALUES(?,?,?,'pending',?,?)
                   ON CONFLICT(legacy_ref_hash) DO UPDATE SET
                       source_kind=excluded.source_kind,
                       reason_code=excluded.reason_code,
                       state=CASE
                           WHEN migration_pending_records.state='dismissed'
                            AND migration_pending_records.source_kind=excluded.source_kind
                            AND migration_pending_records.reason_code=excluded.reason_code
                           THEN 'dismissed'
                           ELSE 'pending'
                       END,
                       updated_at=excluded.updated_at""",
                (reference, source, reason, now, now),
            )
            preserved_dismissal = bool(
                prior is not None
                and prior["state"] == "dismissed"
                and prior["source_kind"] == source
                and prior["reason_code"] == reason
            )
            if not preserved_dismissal and (
                prior is None or prior["source_kind"] != source
                or prior["reason_code"] != reason or prior["state"] != "pending"
            ):
                self._audit(connection, "pending_recorded", reference, reason)
            row = connection.execute(
                "SELECT * FROM migration_pending_records WHERE legacy_ref_hash=?", (reference,)
            ).fetchone()
        return dict(row) if row is not None else {}

    def resolve_pending(self, legacy_ref_hash: str, *, resolution_code: str = "exact_identity_backfilled") -> bool:
        reference = _digest(legacy_ref_hash)
        resolution = _token(resolution_code, 80)
        if not reference or not resolution:
            raise MigrationCoordinatorError("migration_pending_invalid")
        with self._transaction() as connection:
            changed = connection.execute(
                """UPDATE migration_pending_records SET state='resolved',reason_code=?,updated_at=?
                   WHERE legacy_ref_hash=? AND state IN ('pending','dismissed')""",
                (resolution, float(self._clock()), reference),
            ).rowcount
            if changed:
                self._audit(connection, "pending_resolved", reference, resolution)
        return bool(changed)

    def dismiss_pending(
        self, legacy_ref_hash: str, *, resolution_code: str = "administrator_deferred"
    ) -> bool:
        """Hide one exception from the active queue without deleting its evidence."""
        reference = _digest(legacy_ref_hash)
        resolution = _token(resolution_code, 80)
        if not reference or not resolution:
            raise MigrationCoordinatorError("migration_pending_invalid")
        with self._transaction() as connection:
            changed = connection.execute(
                """UPDATE migration_pending_records SET state='dismissed',updated_at=?
                   WHERE legacy_ref_hash=? AND state='pending'""",
                (float(self._clock()), reference),
            ).rowcount
            if changed:
                self._audit(connection, "pending_dismissed", reference, resolution)
        return bool(changed)

    def restore_pending(
        self, legacy_ref_hash: str, *, resolution_code: str = "administrator_restored"
    ) -> bool:
        """Return a dismissed exception to the active review queue."""
        reference = _digest(legacy_ref_hash)
        resolution = _token(resolution_code, 80)
        if not reference or not resolution:
            raise MigrationCoordinatorError("migration_pending_invalid")
        with self._transaction() as connection:
            changed = connection.execute(
                """UPDATE migration_pending_records SET state='pending',updated_at=?
                   WHERE legacy_ref_hash=? AND state='dismissed'""",
                (float(self._clock()), reference),
            ).rowcount
            if changed:
                self._audit(connection, "pending_restored", reference, resolution)
        return bool(changed)

    def pending_summary(self) -> dict[str, Any]:
        with self._connection() as connection:
            rows = connection.execute(
                """SELECT source_kind,reason_code,COUNT(*) AS count
                   FROM migration_pending_records WHERE state='pending'
                   GROUP BY source_kind,reason_code ORDER BY source_kind,reason_code"""
            ).fetchall()
        items = [dict(row) for row in rows]
        return {"total": sum(int(item["count"]) for item in items), "reasons": items}

    def pending_status(self, legacy_ref_hash: str) -> dict[str, Any]:
        """Read one opaque pending record without returning its lookup hash."""
        reference = _digest(legacy_ref_hash)
        if not reference:
            return {"found": False, "state": "invalid", "reason_code": "pending_reference_invalid"}
        with self._connection() as connection:
            row = connection.execute(
                """SELECT source_kind,reason_code,state,first_seen_at,updated_at
                   FROM migration_pending_records WHERE legacy_ref_hash=?""",
                (reference,),
            ).fetchone()
        if row is None:
            return {"found": False, "state": "none", "reason_code": ""}
        return {
            "found": True,
            "source_kind": str(row["source_kind"] or "")[:40],
            "reason_code": str(row["reason_code"] or "")[:80],
            "state": str(row["state"] or "pending")[:24],
            "first_seen_at": float(row["first_seen_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
        }

    def safe_admin_summary(self) -> dict[str, Any]:
        """Return aggregate migration state without identity or source values."""
        with self._connection() as connection:
            identity_rows = connection.execute(
                """SELECT assurance,state,read_generation,COUNT(*) AS count,
                          SUM(backlog) AS backlog,MAX(stable_cycles) AS max_stable_cycles
                   FROM migration_identities
                   GROUP BY assurance,state,read_generation
                   ORDER BY assurance,state,read_generation"""
            ).fetchall()
            lease_count = connection.execute(
                "SELECT COUNT(*) AS count FROM migration_read_leases"
            ).fetchone()
        return {
            "identities": [
                {
                    "assurance": str(row["assurance"]),
                    "state": str(row["state"]),
                    "read_generation": str(row["read_generation"]),
                    "count": int(row["count"] or 0),
                    "backlog": int(row["backlog"] or 0),
                    "max_stable_cycles": int(row["max_stable_cycles"] or 0),
                }
                for row in identity_rows
            ],
            "active_read_leases": int(lease_count["count"] or 0) if lease_count else 0,
            "pending": self.pending_summary(),
        }

    def reconcile_identity(
        self, identity_id: str, *, source_revision: int, target_revision: int,
        source_hash: str, target_hash: str, backlog: int,
    ) -> dict[str, Any]:
        identity = _token(identity_id)
        hashes = (_digest(source_hash), _digest(target_hash))
        if not identity or min(source_revision, target_revision, backlog) < 0 or not all(hashes):
            raise MigrationCoordinatorError("migration_reconcile_invalid")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT assurance,state,read_generation,stable_cycles FROM migration_identities WHERE identity_id=?", (identity,)
            ).fetchone()
            if row is None:
                raise MigrationStateConflict("migration_identity_missing")
            matched = source_revision == target_revision and hashes[0] == hashes[1] and backlog == 0
            formal = row["assurance"] in FORMAL_ASSURANCE
            if not formal:
                state, read_generation = "pending", "legacy"
            elif matched and row["read_generation"] == "new":
                state, read_generation = "new_read", "new"
            elif matched:
                state, read_generation = "reconciling", "legacy"
            elif row["read_generation"] == "new":
                state, read_generation = "legacy_read", "legacy"
                self._audit(connection, "identity_auto_rollback", identity, "migration_reconcile_mismatch")
            else:
                state, read_generation = "reconciling", "legacy"
            stable = int(row["stable_cycles"]) + 1 if matched and formal else 0
            connection.execute(
                """UPDATE migration_identities SET state=?,read_generation=?,source_revision=?,target_revision=?,
                   source_hash=?,target_hash=?,backlog=?,stable_cycles=?,error_code=?,updated_at=?
                   WHERE identity_id=?""",
                (
                    state, read_generation, source_revision, target_revision, hashes[0], hashes[1],
                    backlog, stable, "" if matched else "migration_reconcile_mismatch",
                    float(self._clock()), identity,
                ),
            )
        return self.identity_status(identity)

    def switch_identity_to_new_read(self, identity_id: str, *, required_stable_cycles: int = 1) -> dict[str, Any]:
        identity = _token(identity_id)
        with self._transaction() as connection:
            control = connection.execute("SELECT phase,state FROM migration_control WHERE singleton=1").fetchone()
            if (
                control is None or PHASES.index(control["phase"]) < PHASES.index("S6")
                or control["state"] not in {"active", "replaying"}
            ):
                raise MigrationStateConflict("migration_cutover_phase_denied")
            row = connection.execute("SELECT * FROM migration_identities WHERE identity_id=?", (identity,)).fetchone()
            if row is None:
                raise MigrationStateConflict("migration_identity_missing")
            safe = (
                row["assurance"] in FORMAL_ASSURANCE and row["source_revision"] == row["target_revision"]
                and row["source_hash"] == row["target_hash"] and int(row["backlog"]) == 0
                and int(row["stable_cycles"]) >= max(1, int(required_stable_cycles))
            )
            if not safe:
                raise MigrationStateConflict("migration_identity_not_reconciled")
            connection.execute(
                "UPDATE migration_identities SET state='new_read',read_generation='new',dual_write=1,updated_at=? WHERE identity_id=?",
                (float(self._clock()), identity),
            )
        return self.identity_status(identity)

    def rollback_identity(self, identity_id: str, *, reason_code: str) -> dict[str, Any]:
        identity, reason = _token(identity_id), _token(reason_code, 80)
        if not identity or not reason:
            raise MigrationCoordinatorError("migration_rollback_invalid")
        with self._transaction() as connection:
            changed = connection.execute(
                """UPDATE migration_identities SET state='legacy_read',read_generation='legacy',
                   dual_write=1,error_code=?,updated_at=? WHERE identity_id=?""",
                (reason, float(self._clock()), identity),
            ).rowcount
            if changed != 1:
                raise MigrationStateConflict("migration_identity_missing")
            self._audit(connection, "identity_rollback", identity, reason)
        return self.identity_status(identity)

    def begin_read_chain(self, identity_id: str, chain_id: str) -> str:
        identity, chain = _token(identity_id), _token(chain_id, 160)
        if not identity or not chain:
            raise MigrationCoordinatorError("migration_read_chain_invalid")
        with self._transaction() as connection:
            prior = connection.execute("SELECT identity_id,read_generation FROM migration_read_leases WHERE chain_id=?", (chain,)).fetchone()
            if prior is not None:
                if prior["identity_id"] != identity:
                    raise MigrationStateConflict("migration_read_chain_conflict")
                return prior["read_generation"]
            row = connection.execute("SELECT read_generation FROM migration_identities WHERE identity_id=?", (identity,)).fetchone()
            if row is None:
                raise MigrationStateConflict("migration_identity_missing")
            control = connection.execute(
                "SELECT migration_epoch,state FROM migration_control WHERE singleton=1"
            ).fetchone()
            if control is None:
                raise MigrationStateConflict("migration_control_missing")
            epoch = control["migration_epoch"]
            generation = row["read_generation"] if control["state"] in {"active", "replaying"} else "legacy"
            connection.execute(
                "INSERT INTO migration_read_leases VALUES(?,?,?,?,?)",
                (chain, identity, generation, epoch, float(self._clock())),
            )
            return generation

    def finish_read_chain(self, chain_id: str) -> bool:
        with self._transaction() as connection:
            return connection.execute("DELETE FROM migration_read_leases WHERE chain_id=?", (_token(chain_id, 160),)).rowcount == 1

    def pause(self, reason_code: str) -> dict[str, Any]:
        reason = _token(reason_code, 80) or "migration_paused"
        with self._transaction() as connection:
            connection.execute(
                "UPDATE migration_control SET state='paused',error_code=?,updated_at=? WHERE singleton=1",
                (reason, float(self._clock())),
            )
        return self.status()

    def resume(self) -> dict[str, Any]:
        if not self.verify_backup():
            raise MigrationStateConflict("migration_backup_unverified")
        with self._transaction() as connection:
            connection.execute(
                "UPDATE migration_control SET state='replaying',error_code='',updated_at=? WHERE singleton=1",
                (float(self._clock()),),
            )
        return self.status()

    def resume_if_recoverable(self) -> dict[str, Any]:
        """Clear a replay-class pause so the durable queue can be retried.

        The migration is intentionally fail-closed: a single bad outbox item
        pauses the whole run so no partially applied state is trusted. That is
        correct, but it also means a *code* defect leaves the migration paused
        forever, because ``resume()`` had no caller anywhere in the tree and
        ``_req041_initialize_automatic_migration`` returns early on ``paused``.

        Only replay/apply failures are retried automatically, and only after
        the verified backup still checks out. Integrity pauses (backup or
        source files changed) remain fail-closed for a human to resolve.
        """
        status = self.status()
        if status.get("state") != "paused":
            return {"ok": False, "code": "migration_not_paused", "status": status}
        reason = str(status.get("error_code") or "").strip()
        if reason not in RECOVERABLE_PAUSE_REASONS:
            return {"ok": False, "code": "migration_pause_not_recoverable", "reason": reason, "status": status}
        if not self.verify_backup():
            return {"ok": False, "code": "migration_backup_unverified", "status": status}
        resumed = self.resume()
        return {"ok": True, "code": "migration_replay_resumed", "reason": reason, "status": resumed}

    @staticmethod
    def _audit(connection: sqlite3.Connection, operation: str, identity_id: str, code: str) -> None:
        identity_hash = hashlib.sha256(identity_id.encode("utf-8")).hexdigest()[:16] if identity_id else "none"
        connection.execute(
            "INSERT INTO migration_audit(operation,identity_hash,code,created_at) VALUES(?,?,?,strftime('%s','now'))",
            (operation, identity_hash, code),
        )

    def status(self) -> dict[str, Any]:
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM migration_control WHERE singleton=1").fetchone()
        return dict(row) if row is not None else {}

    def identity_status(self, identity_id: str) -> dict[str, Any]:
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM migration_identities WHERE identity_id=?", (_token(identity_id),)).fetchone()
        return dict(row) if row is not None else {}

    def identity_ids(self) -> list[str]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT identity_id FROM migration_identities ORDER BY identity_id"
            ).fetchall()
        return [str(row["identity_id"]) for row in rows]

    def ready_identity_ids(self, *, required_stable_cycles: int = 2) -> list[str]:
        required = max(1, int(required_stable_cycles))
        with self._connection() as connection:
            rows = connection.execute(
                """SELECT identity_id FROM migration_identities
                   WHERE assurance IN ('verified','explicit_linked')
                     AND state='reconciling' AND source_revision=target_revision
                     AND source_hash<>'' AND source_hash=target_hash AND backlog=0
                     AND stable_cycles>=? ORDER BY identity_id""",
                (required,),
            ).fetchall()
        return [str(row["identity_id"]) for row in rows]

    def prune_read_chains(self, *, older_than: float) -> int:
        cutoff = float(older_than)
        with self._transaction() as connection:
            return connection.execute(
                "DELETE FROM migration_read_leases WHERE created_at<?", (cutoff,)
            ).rowcount


__all__ = [
    "COMPATIBILITY_KEYS", "FORMAL_ASSURANCE", "IDENTITY_STATES", "MigrationCoordinator",
    "MigrationCoordinatorError", "MigrationPreflightError", "MigrationStateConflict", "PHASES", "STATES",
]
