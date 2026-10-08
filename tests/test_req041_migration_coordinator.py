from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from migration_coordinator import (
    MigrationCoordinator,
    MigrationCoordinatorError,
    MigrationPreflightError,
    MigrationStateConflict,
)


class MigrationCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name)
        self.source = self.data_dir / "companions.json"
        self.source.write_text('{"users":{"u1":{"score":3}}}', encoding="utf-8")
        self.clock = [1_786_291_200.0]
        self.coordinator = MigrationCoordinator(self.data_dir, clock=lambda: self.clock[0])

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _start(self, coordinator=None, **changes):
        values = {
            "source_files": [self.source],
            "policy_version": "req041-v1",
            "source_schema_version": "legacy-v2",
            "target_schema_version": "req041-v1",
            "companion_version": "6.1.1",
            "memory_version": "1.7.2",
            "reserve_bytes": 0,
        }
        values.update(changes)
        inventory = values.get("source_inventory")
        if isinstance(inventory, dict) and "source_schema_version" not in changes:
            values["source_schema_version"] = inventory.get("source_schema_version", "")
        return (coordinator or self.coordinator).start_or_resume(**values)

    @staticmethod
    def _inventory(fingerprint: str = "a" * 64) -> dict:
        return {
            "schema": "req041.source_inventory.v1",
            "source_schema_version": f"companion-v1-{fingerprint[:32]}",
            "fingerprint": fingerprint,
            "source_count": 1,
            "formats": {"json": 1, "sqlite": 0},
            "store_version": 1,
            "section_schema_versions": [],
            "all_have_unified_person": True,
            "all_have_persona_lifecycle": True,
            "section_count_min": 5,
            "section_count_max": 5,
        }

    def test_start_creates_verified_immutable_backup_and_restart_reuses_epoch(self) -> None:
        status = self._start()
        self.assertEqual("S1", status["phase"])
        self.assertEqual("backup_verified", status["checkpoint"])
        self.assertTrue(self.coordinator.verify_backup())
        manifest = self.data_dir / status["backup_manifest"]
        backup = manifest.parent / "files" / "companions.json"
        self.assertEqual(self.source.read_bytes(), backup.read_bytes())
        epoch = status["migration_epoch"]
        self.clock[0] += 60
        reopened = MigrationCoordinator(self.data_dir, clock=lambda: self.clock[0])
        resumed = self._start(reopened)
        self.assertEqual(epoch, resumed["migration_epoch"])
        self.assertTrue(reopened.verify_backup())

    def test_fresh_runtime_is_durable_without_creating_a_migration_backup(self) -> None:
        self.source.unlink()
        status = self.coordinator.initialize_fresh_runtime(
            policy_version="req041-v1",
            target_schema_version="req041-v1",
            companion_version="6.1.2",
            memory_version="1.7.2",
        )
        self.assertEqual("S9", status["phase"])
        self.assertEqual("active", status["state"])
        self.assertEqual("req041-fresh-v1", status["source_schema_version"])
        self.assertEqual("", status["backup_manifest"])
        self.assertFalse(self.coordinator.verify_backup())
        self.assertFalse((self.data_dir / "req041_backups").exists())

        reopened = MigrationCoordinator(self.data_dir, clock=lambda: self.clock[0] + 60)
        resumed = reopened.initialize_fresh_runtime(
            policy_version="req041-v1",
            target_schema_version="req041-v1",
            companion_version="6.1.2",
            memory_version="1.7.2",
        )
        self.assertEqual(status["migration_epoch"], resumed["migration_epoch"])
        with self.assertRaisesRegex(MigrationStateConflict, "fresh_runtime_contract_conflict"):
            reopened.initialize_fresh_runtime(
                policy_version="req041-v2",
                target_schema_version="req041-v1",
                companion_version="6.1.2",
                memory_version="1.7.2",
            )

    def test_resume_rejects_tampered_backup_and_changed_source_set(self) -> None:
        status = self._start()
        manifest = self.data_dir / status["backup_manifest"]
        backup = manifest.parent / "files" / "companions.json"
        backup.chmod(0o600)
        backup.write_text("tampered", encoding="utf-8")
        reopened = MigrationCoordinator(self.data_dir)
        with self.assertRaisesRegex(MigrationStateConflict, "migration_backup_unverified"):
            self._start(reopened)
        self.assertEqual("paused", reopened.status()["state"])

        other_dir = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(other_dir, ignore_errors=True))
        other_source = other_dir / "companions.json"
        other_source.write_text("{}", encoding="utf-8")
        other = MigrationCoordinator(other_dir)
        self._start(other, source_files=[other_source])
        added = other_dir / "extra.json"
        added.write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(MigrationStateConflict, "migration_source_set_changed"):
            self._start(other, source_files=[other_source, added])

    def test_recoverable_replay_pause_resumes_and_drains(self) -> None:
        """A replay-class pause must clear so a fixed build can drain the queue."""
        self._start()
        self.coordinator.capture_compatibility({"relationship_policy": {"mode": "legacy"}})
        for phase in ("S3", "S4", "S5", "S6"):
            self.coordinator.transition(phase, checkpoint=phase.lower())
        self.coordinator.pause("relationship_legacy_event_invalid")

        result = self.coordinator.resume_if_recoverable()

        self.assertTrue(result["ok"])
        self.assertEqual("migration_replay_resumed", result["code"])
        self.assertEqual("relationship_legacy_event_invalid", result["reason"])
        self.assertEqual("replaying", self.coordinator.status()["state"])
        self.assertEqual("", self.coordinator.status()["error_code"])

    def test_integrity_pause_stays_fail_closed(self) -> None:
        """Backup/source integrity pauses must not self-heal."""
        self._start()
        self.coordinator.pause("migration_source_set_changed")

        result = self.coordinator.resume_if_recoverable()

        self.assertFalse(result["ok"])
        self.assertEqual("migration_pause_not_recoverable", result["code"])
        self.assertEqual("paused", self.coordinator.status()["state"])

    def test_recoverable_pause_still_requires_a_verified_backup(self) -> None:
        status = self._start()
        manifest = self.data_dir / status["backup_manifest"]
        backup = manifest.parent / "files" / "companions.json"
        backup.chmod(0o600)
        backup.write_text("tampered", encoding="utf-8")
        reopened = MigrationCoordinator(self.data_dir)
        reopened.pause("relationship_legacy_event_invalid")

        result = reopened.resume_if_recoverable()

        self.assertFalse(result["ok"])
        self.assertEqual("migration_backup_unverified", result["code"])
        self.assertEqual("paused", reopened.status()["state"])

    def test_inventory_is_bound_to_manifest_and_resume_contract(self) -> None:
        inventory = self._inventory()
        status = self._start(source_inventory=inventory)
        manifest = __import__("json").loads(
            (self.data_dir / status["backup_manifest"]).read_text(encoding="utf-8")
        )
        self.assertEqual("req041.backup_manifest.v2", manifest["schema"])
        self.assertEqual(inventory, manifest["source_inventory"])

        changed = self._inventory()
        changed["section_count_max"] = 6
        reopened = MigrationCoordinator(self.data_dir)
        resumed = self._start(reopened, source_inventory=changed)
        self.assertEqual(status["migration_epoch"], resumed["migration_epoch"])

        other_dir = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(other_dir, ignore_errors=True))
        other_source = other_dir / "companions.json"
        other_source.write_text(self.source.read_text(encoding="utf-8"), encoding="utf-8")
        other = MigrationCoordinator(other_dir)
        self._start(other, source_files=[other_source], source_inventory=inventory)
        with self.assertRaisesRegex(MigrationStateConflict, "migration_resume_contract_conflict"):
            self._start(
                other,
                source_files=[other_source],
                source_inventory=self._inventory("b" * 64),
            )

    def test_rejects_malformed_source_inventory_before_backup(self) -> None:
        malformed = self._inventory()
        malformed["formats"] = {"json": 0, "sqlite": 0}
        with self.assertRaisesRegex(MigrationPreflightError, "migration_source_inventory_invalid"):
            self._start(source_inventory=malformed)
        self.assertEqual({}, self.coordinator.status())

    def test_crash_after_s0_resumes_same_epoch_and_finishes_backup(self) -> None:
        with patch.object(self.coordinator, "_create_verified_backup", side_effect=RuntimeError("crash")):
            with self.assertRaisesRegex(RuntimeError, "crash"):
                self._start()
        before = self.coordinator.status()
        self.assertEqual("S0", before["phase"])
        reopened = MigrationCoordinator(self.data_dir, clock=lambda: self.clock[0] + 10)
        after = self._start(reopened)
        self.assertEqual(before["migration_epoch"], after["migration_epoch"])
        self.assertEqual("S1", after["phase"])

    def test_sqlite_source_uses_online_backup_and_contains_wal_commits(self) -> None:
        sqlite_dir = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(sqlite_dir, ignore_errors=True))
        database = sqlite_dir / "companions.db"
        connection = sqlite3.connect(database)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE users(id TEXT PRIMARY KEY, score INTEGER)")
        connection.execute("INSERT INTO users VALUES('u1', 7)")
        connection.commit()
        coordinator = MigrationCoordinator(sqlite_dir)
        status = self._start(coordinator, source_files=[database])
        backup = sqlite_dir / status["backup_manifest"]
        copied = backup.parent / "files" / "companions.db"
        copied_connection = sqlite3.connect(copied)
        try:
            self.assertEqual(("u1", 7), copied_connection.execute("SELECT * FROM users").fetchone())
            self.assertEqual("ok", copied_connection.execute("PRAGMA quick_check").fetchone()[0])
        finally:
            copied_connection.close()
            connection.close()

    def test_preflight_rejects_symlink_escape_and_insufficient_space(self) -> None:
        outside = self.data_dir.parent / f"{self.data_dir.name}-outside.json"
        outside.write_text("{}", encoding="utf-8")
        self.addCleanup(outside.unlink, missing_ok=True)
        link = self.data_dir / "escape.json"
        link.symlink_to(outside)
        with self.assertRaisesRegex(MigrationPreflightError, "migration_source_file_invalid"):
            self.coordinator.preflight([link], reserve_bytes=0)
        with self.assertRaisesRegex(MigrationPreflightError, "migration_space_insufficient"):
            self.coordinator.preflight([self.source], reserve_bytes=10**30)

    def test_compatibility_snapshot_is_allowlisted_and_backup_tamper_stops_progress(self) -> None:
        status = self._start()
        snapshot = self.coordinator.capture_compatibility({
            "auto_profile_creation": True,
            "private_access_policy": {"mode": "legacy_open"},
            "owner_policy": {"mode": "normal", "positive_cap_exempt": True},
        })
        self.assertEqual("legacy_open", snapshot["private_access_policy"]["mode"])
        with self.assertRaisesRegex(MigrationCoordinatorError, "compatibility_snapshot_keys_invalid"):
            self.coordinator.capture_compatibility({"unknown_policy": True})
        with self.assertRaisesRegex(MigrationCoordinatorError, "compatibility_snapshot_forbidden"):
            self.coordinator.capture_compatibility({"tool_policy": {"api_token": "secret"}})
        manifest = self.data_dir / status["backup_manifest"]
        backup = manifest.parent / "files" / "companions.json"
        backup.chmod(0o600)
        backup.write_text("tampered", encoding="utf-8")
        self.assertFalse(self.coordinator.verify_backup())
        with self.assertRaisesRegex(MigrationStateConflict, "migration_backup_unverified"):
            self.coordinator.transition("S3", checkpoint="dual_write")

    def test_identity_switch_requires_formal_exact_reconciliation_and_zero_backlog(self) -> None:
        self._start()
        pending = self.coordinator.register_identity("person-pending", assurance="observed")
        self.assertEqual("pending", pending["state"])
        self.coordinator.reconcile_identity(
            "person-pending", source_revision=1, target_revision=1,
            source_hash="a" * 64, target_hash="a" * 64, backlog=0,
        )
        self.coordinator.capture_compatibility({"relationship_policy": {"mode": "legacy"}})
        for phase in ("S3", "S4", "S5", "S6"):
            self.coordinator.transition(phase, checkpoint=phase.lower())
        with self.assertRaisesRegex(MigrationStateConflict, "migration_identity_not_reconciled"):
            self.coordinator.switch_identity_to_new_read("person-pending")

        identity = self.coordinator.register_identity("person-a", assurance="verified")
        self.assertEqual("legacy", identity["read_generation"])
        self.assertEqual("legacy", self.coordinator.begin_read_chain("person-a", "chain-before"))
        mismatch = self.coordinator.reconcile_identity(
            "person-a", source_revision=2, target_revision=1,
            source_hash="a" * 64, target_hash="b" * 64, backlog=1,
        )
        self.assertEqual(0, mismatch["stable_cycles"])
        with self.assertRaisesRegex(MigrationStateConflict, "migration_identity_not_reconciled"):
            self.coordinator.switch_identity_to_new_read("person-a")
        self.coordinator.reconcile_identity(
            "person-a", source_revision=2, target_revision=2,
            source_hash="c" * 64, target_hash="c" * 64, backlog=0,
        )
        reconciled = self.coordinator.reconcile_identity(
            "person-a", source_revision=2, target_revision=2,
            source_hash="c" * 64, target_hash="c" * 64, backlog=0,
        )
        self.assertEqual(2, reconciled["stable_cycles"])
        switched = self.coordinator.switch_identity_to_new_read("person-a", required_stable_cycles=2)
        self.assertEqual("new", switched["read_generation"])
        self.assertEqual("legacy", self.coordinator.begin_read_chain("person-a", "chain-before"))
        self.assertEqual("new", self.coordinator.begin_read_chain("person-a", "chain-after"))
        self.coordinator.pause("test_global_pause")
        self.assertEqual("new", self.coordinator.begin_read_chain("person-a", "chain-after"))
        self.assertEqual("legacy", self.coordinator.begin_read_chain("person-a", "chain-paused"))
        self.coordinator.resume()
        auto_rollback = self.coordinator.reconcile_identity(
            "person-a", source_revision=3, target_revision=2,
            source_hash="d" * 64, target_hash="c" * 64, backlog=1,
        )
        self.assertEqual("legacy", auto_rollback["read_generation"])
        self.assertEqual("legacy_read", auto_rollback["state"])
        self.assertEqual("new", self.coordinator.begin_read_chain("person-a", "chain-after"))
        self.assertEqual("legacy", self.coordinator.begin_read_chain("person-a", "chain-after-rollback"))
        downgraded = self.coordinator.register_identity("person-a", assurance="observed")
        self.assertEqual("legacy", downgraded["read_generation"])
        self.assertEqual("pending", downgraded["state"])
        self.coordinator.register_identity("person-a", assurance="verified")
        rolled_back = self.coordinator.rollback_identity("person-a", reason_code="shadow_mismatch")
        self.assertEqual("legacy", rolled_back["read_generation"])
        self.assertTrue(self.coordinator.finish_read_chain("chain-before"))

    def test_phase_pause_resume_and_backward_transition_are_guarded(self) -> None:
        self._start()
        self.coordinator.capture_compatibility({"relationship_policy": {"mode": "legacy"}})
        self.assertEqual("S3", self.coordinator.transition("S3", checkpoint="dual_write")["phase"])
        with self.assertRaisesRegex(MigrationStateConflict, "migration_transition_denied"):
            self.coordinator.transition("S2", checkpoint="backward")
        paused = self.coordinator.pause("outbox_backlog")
        self.assertEqual("paused", paused["state"])
        with self.assertRaisesRegex(MigrationStateConflict, "migration_transition_denied"):
            self.coordinator.transition("S4", checkpoint="backfill")
        resumed = self.coordinator.resume()
        self.assertEqual("replaying", resumed["state"])
        self.assertEqual("S4", self.coordinator.transition("S4", checkpoint="backfill")["phase"])


if __name__ == "__main__":
    unittest.main()
