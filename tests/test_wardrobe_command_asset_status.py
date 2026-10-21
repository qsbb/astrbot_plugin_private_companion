"""Issue #274: chat-applied images must not remain unrecognized drafts."""
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from astrbot_plugin_private_companion import wardrobe_runtime


class Host(wardrobe_runtime.WardrobeMixin):
    data_dir = "unused-test-path"

    def __init__(self, *, save=True, parsed=True):
        self.save = save
        self.parsed = parsed
        self.saved = False

    def _wardrobe_enabled(self):
        return True

    def _wardrobe_image_limit(self):
        return 2

    async def _photo_reference_images_from_command_context(self, *args, **kwargs):
        return [("image.png", "test")], True

    def _wardrobe_items(self):
        return []

    def _wardrobe_outfits(self):
        return []

    async def _wardrobe_describe_image(self, *args, **kwargs):
        return ({"kind": "outfit", "name": "test"} if self.parsed else None), "识图失败"

    def _import_wardrobe_asset(self, *args, **kwargs):
        return "asset-test"

    async def _save_wardrobe_state(self, **kwargs):
        self.saved = self.save
        return self.save


@pytest.mark.asyncio
@pytest.mark.parametrize("save,apply,expected", [(True, True, 1), (False, True, 0), (True, False, 0)])
async def test_asset_advanced_only_after_successful_apply_and_save(save, apply, expected):
    host = Host(save=save)

    def mark(*args):
        assert host.saved

    with patch.object(wardrobe_runtime, "apply_wardrobe_draft", return_value=([], [], {
        "ok": apply, "kind": "outfit", "name": "test", "error": "failed",
    })), patch.object(wardrobe_runtime, "mark_asset_status", side_effect=mark) as status:
        await host._wardrobe_add_from_image(SimpleNamespace(unified_msg_origin="test"), "user", "")
    assert status.call_count == expected
    if expected:
        assert status.call_args.args == (
            host.data_dir, "asset-test", wardrobe_runtime.ASSET_STATUS_UNDERSTOOD,
        )


@pytest.mark.asyncio
async def test_failed_vision_does_not_mark_asset_understood():
    host = Host(parsed=False)
    with patch.object(wardrobe_runtime, "mark_asset_status") as status:
        await host._wardrobe_add_from_image(SimpleNamespace(unified_msg_origin="test"), "user", "")
    status.assert_not_called()
