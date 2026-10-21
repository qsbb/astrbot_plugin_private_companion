"""Issue #273: missing frozen sources have a specific, fail-closed error."""
import pytest

from migration_coordinator import MigrationCoordinator, MigrationPreflightError
from migration_source_inspector import MigrationSourceInspectionError, inspect_migration_sources


def test_inspection_reports_missing_source_without_path_details(tmp_path):
    with pytest.raises(MigrationSourceInspectionError, match="^migration_source_file_missing$"):
        inspect_migration_sources(tmp_path, [tmp_path / "persona_profiles/deleted.db"])


def test_coordinator_keeps_verified_manifest_when_frozen_source_disappears(tmp_path):
    source = tmp_path / "companions.json"
    source.write_text('{"users":{"user":{"score":3}}}', encoding="utf-8")
    coordinator = MigrationCoordinator(tmp_path)
    args = dict(source_files=[source], policy_version="req041-v1", source_schema_version="legacy-v2",
                target_schema_version="req041-v1", companion_version="6.7.0", memory_version="2.1.4",
                reserve_bytes=0)
    status = coordinator.start_or_resume(**args)
    manifest = tmp_path / status["backup_manifest"]
    before = manifest.read_bytes()
    source.rename(tmp_path / "deleted-companions.json")
    with pytest.raises(MigrationPreflightError, match="^migration_source_file_missing$"):
        coordinator.start_or_resume(**args)
    assert manifest.read_bytes() == before
    assert coordinator.verify_backup()


def test_missing_external_path_is_still_rejected_as_path_escape(tmp_path):
    outside = tmp_path.parent / "nonexistent-outside-source.json"
    with pytest.raises(MigrationSourceInspectionError, match="^migration_source_path_invalid$"):
        inspect_migration_sources(tmp_path, [outside])
