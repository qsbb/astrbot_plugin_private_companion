"""Issue #272: persona authority is computed once per tree, not per leaf."""
from copy import deepcopy
from unittest.mock import patch

from astrbot_plugin_private_companion.daily_state import DailyStateMixin


class Host(DailyStateMixin):
    authority = ""

    def _daily_plan_relationship_authority_sources(self):
        return (self.authority,)


def test_thousands_of_nested_strings_share_one_authority_computation():
    host = Host()
    tree = {"history": [{"text": "天气不错", "nested": ["普通生活", "慢慢散步"]} for _ in range(2000)]}
    before = deepcopy(tree)
    with patch.object(host, "_daily_plan_declared_relation_tokens", wraps=host._daily_plan_declared_relation_tokens) as declared:
        assert host._sanitize_relationship_text_tree_inplace(tree, field="history") is False
    assert declared.call_count == 1
    assert tree == before


def test_persona_changes_are_seen_on_the_next_traversal():
    host = Host()
    first = {"text": "妈妈在家"}
    host._sanitize_relationship_text_tree_inplace(first, field="history")
    host.authority = "角色与妈妈一起生活"
    second = {"text": "妈妈在家"}
    host._sanitize_relationship_text_tree_inplace(second, field="history")
    assert first["text"] != second["text"]
    assert second["text"] == "妈妈在家"


def test_tree_cleanup_matches_uncached_clause_cleanup_and_preserves_raw_evidence():
    host = Host()
    texts = ["妈妈在家，天气不错。", "用户的妈妈在家。", "慢慢散步", "爸爸在家。"]
    expected = [host._sanitize_generation_relationship_context(text) for text in texts]
    tree = {"texts": list(texts), "raw_text": "妈妈在家。", "prompt": "爸爸在家。"}
    host._sanitize_relationship_text_tree_inplace(tree, field="history")
    assert tree["texts"] == [text for text in expected if text]
    assert tree["raw_text"] == "妈妈在家。"
    assert tree["prompt"] == "爸爸在家。"
