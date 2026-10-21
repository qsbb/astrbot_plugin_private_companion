"""REQ-041 authoritative person-private memory with revision/CAS semantics."""
from __future__ import annotations

from copy import deepcopy
from functools import wraps
import hashlib
import json
import math
import time
from threading import RLock
from typing import Any


PRIVATE_MEMORY_FIELDS = (
    "companion_memory",
    "intent_profile",
    "dialogue_episodes",
    "open_loops",
    "behavior_habits",
    "action_preferences",
    "action_consequences",
    "state_continuity",
    "recent_reply_topics",
    "birthday_profile",
    "birthday_curiosity_opt_out",
    "birthday_curiosity_asked_at",
    "birthday_curiosity_answered_at",
    "episode_message_count",
    "last_episode_refresh_at",
    "dialogue_episode_retry_after",
    "dialogue_episode_last_error",
    "dialogue_episode_running_at",
    "last_memory_refresh_at",
    "companion_memory_retry_after",
    "companion_memory_last_error",
    "companion_memory_running_at",
)
_ROOT_KEY = "_req041_private_memory"
_SCHEMA = "req041.person_private_memory.v1"
_OPERATION_LOG_LIMIT = 32

# A bounded process-local lock pool protects independent Store instances sharing
# the same snapshot. Never put locks inside persisted user data or rely on an
# instance lock: callers routinely construct a new Store for each operation.
_SNAPSHOT_LOCKS = tuple(RLock() for _ in range(64))


def _snapshot_locked(method):
    @wraps(method)
    def guarded(self, *args, **kwargs):
        with _SNAPSHOT_LOCKS[(id(self.snapshot) >> 4) % len(_SNAPSHOT_LOCKS)]:
            return method(self, *args, **kwargs)
    return guarded


class AuthoritativePrivateMemoryError(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _token(value: Any, limit: int = 128) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if not text or len(text) > limit or any(ord(char) < 32 for char in text):
        return ""
    return text


def _bounded(value: Any, *, depth: int = 0) -> Any:
    if depth > 8:
        raise AuthoritativePrivateMemoryError("private_memory_depth_exceeded")
    if value is None or isinstance(value, (bool, int, str)):
        return value[:8000] if isinstance(value, str) else value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise AuthoritativePrivateMemoryError("private_memory_number_invalid")
        return value
    if isinstance(value, list):
        return [_bounded(item, depth=depth + 1) for item in value[-512:]]
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in list(value.items())[:512]:
            if not isinstance(key, str) or not key or len(key) > 128:
                raise AuthoritativePrivateMemoryError("private_memory_key_invalid")
            result[key] = _bounded(item, depth=depth + 1)
        return result
    raise AuthoritativePrivateMemoryError("private_memory_value_invalid")


def private_memory_content(user: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(user, dict):
        raise AuthoritativePrivateMemoryError("private_memory_user_invalid")
    return {
        field: _bounded(deepcopy(user[field]))
        for field in PRIVATE_MEMORY_FIELDS
        if field in user and user[field] not in (None, "", [], {})
    }


def apply_private_memory_content(user: dict[str, Any], content: dict[str, Any]) -> None:
    if not isinstance(user, dict) or not isinstance(content, dict):
        raise AuthoritativePrivateMemoryError("private_memory_content_invalid")
    unexpected = set(content) - set(PRIVATE_MEMORY_FIELDS)
    if unexpected:
        raise AuthoritativePrivateMemoryError("private_memory_fields_invalid")
    for field in PRIVATE_MEMORY_FIELDS:
        if field in content:
            user[field] = _bounded(deepcopy(content[field]))
        else:
            user.pop(field, None)


def _write_fields(fields: Any) -> tuple[str, ...]:
    """Normalize a declared write set; ``None`` means the whole memory field list."""
    if fields is None:
        return PRIVATE_MEMORY_FIELDS
    if isinstance(fields, str) or not isinstance(fields, (list, tuple, set, frozenset)):
        raise AuthoritativePrivateMemoryError("private_memory_fields_invalid")
    requested = set(fields)
    if requested - set(PRIVATE_MEMORY_FIELDS):
        raise AuthoritativePrivateMemoryError("private_memory_fields_invalid")
    return tuple(field for field in PRIVATE_MEMORY_FIELDS if field in requested)


def _operation_log(record: Any) -> dict[str, Any]:
    operations = record.get("operations") if isinstance(record, dict) else None
    result = operations if isinstance(operations, dict) else {}
    if not isinstance(record, dict):
        return result
    legacy_operation = record.get("last_operation_hash")
    legacy_request = record.get("last_request_hash")
    if (
        isinstance(legacy_operation, str)
        and legacy_operation
        and isinstance(legacy_request, str)
        and legacy_request
        and legacy_operation not in result
    ):
        result[legacy_operation] = {
            "request_hash": legacy_request,
            "revision": int(record.get("revision") or 0),
            "recorded_at": float(record.get("updated_at") or 0.0),
            "hash_version": "legacy_v1",
        }
    return result


def _field_revisions(record: Any) -> dict[str, int]:
    """Per-field last-write revision; legacy records fall back to a conservative backfill."""
    current_revision = int(record.get("revision") or 0) if isinstance(record, dict) else 0
    raw = record.get("field_revisions") if isinstance(record, dict) else None
    if not isinstance(raw, dict):
        # Legacy commits replaced the whole record, so an absent field was an
        # explicit deletion at the record revision. Mark every field to keep
        # stale post-upgrade writers fail-closed.
        return {
            field: current_revision
            for field in PRIVATE_MEMORY_FIELDS
            if current_revision > 0
        }
    revisions = {
        field: int(value)
        for field, value in raw.items()
        if isinstance(value, int) and not isinstance(value, bool)
    }
    content = record.get("content") if isinstance(record, dict) else None
    content = content if isinstance(content, dict) else {}
    for field in content:
        revisions.setdefault(field, current_revision)
    return revisions


def _record_operation(
    operations: dict[str, Any], operation_hash: str, request_hash: str, revision: int, now: float,
) -> None:
    """Append to the bounded idempotency log; only hashes are persisted."""
    operations[operation_hash] = {
        "request_hash": request_hash,
        "revision": revision,
        "recorded_at": now,
    }
    for stale in list(operations)[:-_OPERATION_LOG_LIMIT]:
        operations.pop(stale, None)


class AuthoritativePrivateMemoryStore:
    def __init__(self, snapshot: dict[str, Any], *, clock: Any = None) -> None:
        if not isinstance(snapshot, dict):
            raise AuthoritativePrivateMemoryError("private_memory_snapshot_invalid")
        self.snapshot = snapshot
        self._clock = clock if callable(clock) else time.time

    def _root(self, *, create: bool = True) -> dict[str, Any] | None:
        root = self.snapshot.get(_ROOT_KEY)
        if root is None:
            if not create:
                return None
            root = {"schema": _SCHEMA, "records": {}}
            self.snapshot[_ROOT_KEY] = root
        if (
            not isinstance(root, dict)
            or root.get("schema") != _SCHEMA
            or not isinstance(root.get("records"), dict)
        ):
            raise AuthoritativePrivateMemoryError("private_memory_store_invalid")
        return root

    @_snapshot_locked
    def read(self, person_id: str) -> dict[str, Any]:
        person = _token(person_id, 80)
        if not person:
            raise AuthoritativePrivateMemoryError("private_memory_person_invalid")
        root = self._root(create=False)
        if root is None:
            return {"ok": True, "code": "not_found", "record": None}
        raw = root["records"].get(person)
        if raw is None:
            return {"ok": True, "code": "not_found", "record": None}
        if not isinstance(raw, dict):
            raise AuthoritativePrivateMemoryError("private_memory_record_not_dict")
        content = raw.get("content")
        revision = raw.get("revision")
        # Split one opaque code into the four conditions it used to cover.  The
        # original single code made a field report impossible to act on: it
        # could not be told apart from "content is not a dict", "revision is
        # illegal", "schema mismatch" or "content_hash mismatch".  The message
        # carries the structural shape of the offending record, never its
        # memory text.
        if not isinstance(content, dict):
            raise AuthoritativePrivateMemoryError(
                "private_memory_content_not_dict: content_type=%s raw_keys=%s"
                % (type(content).__name__, sorted(raw)[:12])
            )
        if (
            not isinstance(revision, int)
            or isinstance(revision, bool)
            or revision < 1
        ):
            raise AuthoritativePrivateMemoryError(
                "private_memory_revision_invalid: revision=%r revision_type=%s"
                % (revision, type(revision).__name__)
            )
        if raw.get("schema") != _SCHEMA:
            raise AuthoritativePrivateMemoryError(
                "private_memory_schema_mismatch: schema=%r expected=%r"
                % (raw.get("schema"), _SCHEMA)
            )
        if raw.get("content_hash") != _digest(content):
            raise AuthoritativePrivateMemoryError(
                "private_memory_hash_mismatch: stored=%r recomputed=%r revision=%r content_keys=%s"
                % (
                    raw.get("content_hash"),
                    _digest(content),
                    revision,
                    sorted(content)[:12],
                )
            )
        return {"ok": True, "code": "found", "record": deepcopy(raw)}

    @_snapshot_locked
    def commit(
        self,
        person_id: str,
        content: dict[str, Any],
        *,
        expected_revision: int,
        operation_id: str,
        fields: Any = None,
    ) -> dict[str, Any]:
        """Commit a field-scoped delta.

        ``fields`` declares the write set owned by this writer. Fields listed there but absent
        from ``content`` are deleted; fields outside the write set are left untouched. ``None``
        keeps the historical whole-record replacement contract.
        """
        person = _token(person_id, 80)
        operation = _token(operation_id, 160)
        if not person or not operation or isinstance(expected_revision, bool) or expected_revision < 0:
            raise AuthoritativePrivateMemoryError("private_memory_commit_invalid")
        if not isinstance(content, dict) or set(content) - set(PRIVATE_MEMORY_FIELDS):
            raise AuthoritativePrivateMemoryError("private_memory_fields_invalid")
        write_fields = _write_fields(fields)
        delta = {
            field: _bounded(deepcopy(content[field]))
            for field in write_fields
            if field in content
        }
        deleted_fields = tuple(field for field in write_fields if field not in delta)
        content_hash = _digest({"set": delta, "delete": list(deleted_fields)})
        operation_hash = hashlib.sha256(operation.encode("utf-8")).hexdigest()
        request_hash = _digest({
            "person_id": person,
            "expected_revision": expected_revision,
            "content_hash": content_hash,
        })
        root = self._root()
        assert root is not None
        records = root["records"]
        current = records.get(person)
        # A write must not silently bless corrupted pre-existing content by
        # computing a fresh hash. Validate under the same snapshot lock first.
        if current is not None:
            self.read(person)
        current_revision = int(current.get("revision") or 0) if isinstance(current, dict) else 0
        # 1) Idempotency first: a replay of an accepted operation always wins, revision aside.
        operations = _operation_log(current)
        prior = operations.get(operation_hash)
        if isinstance(prior, dict):
            prior_request_hash = prior.get("request_hash")
            request_matches = prior_request_hash == request_hash
            if prior.get("hash_version") == "legacy_v1":
                legacy_request_hash = _digest({
                    "person_id": person,
                    "expected_revision": expected_revision,
                    "content_hash": _digest(delta),
                })
                request_matches = fields is None and prior_request_hash == legacy_request_hash
            if not request_matches:
                return {"ok": False, "code": "operation_id_conflict", "revision": current_revision}
            return {
                "ok": True,
                "code": "idempotent",
                "revision": current_revision,
                "record": deepcopy(current),
            }
        # 2) CAS inside this critical section, scoped to the declared write set: a writer may
        #    converge onto a newer revision, but never onto fields another writer just changed.
        field_revisions = _field_revisions(current)
        if current_revision != expected_revision:
            if current_revision < expected_revision or any(
                field_revisions.get(field, 0) > expected_revision for field in write_fields
            ):
                return {
                    "ok": False,
                    "code": "private_memory_revision_conflict",
                    "revision": current_revision,
                }
        current_content = current.get("content") if isinstance(current, dict) else None
        merged = {
            field: deepcopy(value)
            for field, value in (current_content if isinstance(current_content, dict) else {}).items()
        }
        merged.update(deepcopy(delta))
        for field in deleted_fields:
            merged.pop(field, None)
        merged_hash = _digest(merged)
        now = float(self._clock())
        if isinstance(current, dict) and current.get("content_hash") == merged_hash:
            _record_operation(operations, operation_hash, request_hash, current_revision, now)
            current["operations"] = operations
            return {
                "ok": True,
                "code": "unchanged",
                "revision": current_revision,
                "record": deepcopy(current),
            }
        revision = current_revision + 1
        for field in write_fields:
            # Keep a tombstone revision for deletions. Without it, a writer
            # based on an older revision could recreate a field that a newer
            # commit deliberately removed and bypass same-field CAS.
            field_revisions[field] = revision
        _record_operation(operations, operation_hash, request_hash, revision, now)
        record = {
            "schema": _SCHEMA,
            "revision": revision,
            "content": merged,
            "content_hash": merged_hash,
            "updated_at": now,
            "field_revisions": field_revisions,
            "operations": operations,
        }
        records[person] = record
        return {
            "ok": True,
            "code": "created" if current_revision == 0 else "updated",
            "revision": record["revision"],
            "record": deepcopy(record),
        }


__all__ = [
    "AuthoritativePrivateMemoryError",
    "AuthoritativePrivateMemoryStore",
    "PRIVATE_MEMORY_FIELDS",
    "apply_private_memory_content",
    "private_memory_content",
]
