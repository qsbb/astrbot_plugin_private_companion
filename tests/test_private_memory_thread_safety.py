"""Shared snapshot CAS must be atomic across independently constructed stores."""
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from authoritative_private_memory import AuthoritativePrivateMemoryError, AuthoritativePrivateMemoryStore


@pytest.mark.parametrize("same_field", [False, True])
def test_second_thread_cannot_pass_cas_before_first_commit_finishes(same_field):
    snapshot = {}
    AuthoritativePrivateMemoryStore(snapshot).commit("person", {}, expected_revision=0, operation_id="seed")
    first_in_clock, release_first, second_started, second_in_clock = (Event() for _ in range(4))

    def first_clock():
        first_in_clock.set()
        assert release_first.wait(5)
        return 100.0

    def second_write():
        second_started.set()
        def second_clock():
            second_in_clock.set()
            return 101.0
        field = "open_loops" if same_field else "behavior_habits"
        return AuthoritativePrivateMemoryStore(snapshot, clock=second_clock).commit(
            "person", {field: ["second"]}, fields=[field], expected_revision=1, operation_id="second",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(AuthoritativePrivateMemoryStore(snapshot, clock=first_clock).commit,
                            "person", {"open_loops": ["first"]}, fields=["open_loops"],
                            expected_revision=1, operation_id="first")
        try:
            assert first_in_clock.wait(5)
            second = pool.submit(second_write)
            assert second_started.wait(5)
            assert not second_in_clock.wait(0.1)
        finally:
            release_first.set()
        assert first.result(timeout=5)["revision"] == 2
        result = second.result(timeout=5)
    record = AuthoritativePrivateMemoryStore(snapshot).read("person")["record"]
    assert record["content"]["open_loops"] == ["first"]
    if same_field:
        assert result["code"] == "private_memory_revision_conflict"
        assert record["revision"] == 2
    else:
        assert result["ok"] and result["revision"] == 3
        assert record["content"]["behavior_habits"] == ["second"]


def test_lock_is_released_after_invalid_record_and_snapshot_is_serializable():
    import json
    snapshot = {}
    store = AuthoritativePrivateMemoryStore(snapshot)
    store.commit("person", {"open_loops": ["original"]}, expected_revision=0, operation_id="seed")
    snapshot["_req041_private_memory"]["records"]["person"]["content"]["open_loops"] = ["tampered"]
    with pytest.raises(AuthoritativePrivateMemoryError, match="private_memory_hash_mismatch"):
        store.read("person")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(AuthoritativePrivateMemoryStore(snapshot).read, "person")
        with pytest.raises(AuthoritativePrivateMemoryError, match="private_memory_hash_mismatch"):
            future.result(timeout=5)
    json.dumps(snapshot)


def test_commit_cannot_rehash_and_accept_preexisting_corruption():
    from copy import deepcopy
    snapshot = {}
    store = AuthoritativePrivateMemoryStore(snapshot)
    store.commit("person", {"open_loops": ["original"]}, expected_revision=0, operation_id="seed")
    snapshot["_req041_private_memory"]["records"]["person"]["content"]["open_loops"] = ["tampered"]
    before = deepcopy(snapshot)
    with pytest.raises(AuthoritativePrivateMemoryError, match="private_memory_hash_mismatch"):
        store.commit("person", {"behavior_habits": ["new"]}, fields=["behavior_habits"],
                     expected_revision=1, operation_id="new")
    assert snapshot == before
