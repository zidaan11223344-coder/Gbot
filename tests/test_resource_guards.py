import importlib.util
import logging
import logging.handlers
import os
import sys
import time
import types
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


def load_bot_module(poll_value=None):
    fake_dotenv = types.ModuleType("dotenv")
    fake_dotenv.load_dotenv = lambda *args, **kwargs: None

    fake_supabase = types.ModuleType("supabase")

    class FakeOptions:
        def __init__(self):
            self.headers = {}

    class FakeClient:
        def __init__(self):
            self.options = FakeOptions()
            self.supabase_key = None

    fake_supabase.Client = FakeClient
    fake_supabase.create_client = lambda *args, **kwargs: FakeClient()
    env = {
        "SUPABASE_URL": "https://example.supabase.co",
        "SUPABASE_KEY": "test-key",
        "GIANT_USERNAME": "test-user",
        "GIANT_PASSWORD": "test-password",
    }
    if poll_value is not None:
        env["CONTROL_POLL_SECONDS"] = str(poll_value)

    with patch.dict(os.environ, env, clear=True), patch.dict(
        sys.modules, {"dotenv": fake_dotenv, "supabase": fake_supabase}
    ), patch.object(logging, "basicConfig"), patch.object(
        logging.handlers,
        "RotatingFileHandler",
        lambda *args, **kwargs: logging.NullHandler(),
    ):
        name = f"control_bot_test_{time.time_ns()}"
        spec = importlib.util.spec_from_file_location(name, ROOT / "control_bot.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
        finally:
            sys.modules.pop(name, None)
    return module


class ResourceGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot = load_bot_module()

    def setUp(self):
        self.bot.PENDING_COMMANDS.clear()
        self.bot.ROOM_MEMBER_PAGES.clear()

    def test_poll_defaults_to_documented_two_seconds(self):
        self.assertEqual(self.bot.POLL, 2.0)

    def test_poll_rejects_subsecond_configuration(self):
        bot = load_bot_module(0.2)
        self.assertEqual(bot.POLL, 1.0)

    def test_invalid_poll_configuration_falls_back_safely(self):
        for value in ("not-a-number", "inf"):
            with self.subTest(value=value):
                bot = load_bot_module(value)
                self.assertEqual(bot.POLL, 2.0)

    def test_dotenv_import_has_declared_dependency(self):
        requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")
        self.assertRegex(requirements, r"(?m)^python-dotenv(?:[<=>!~].*)?$")

    def test_pending_cache_is_bounded_and_expired_entries_are_removed(self):
        now = time.time()
        for i in range(self.bot.MAX_CACHE_ENTRIES + 10):
            self.bot._set_pending_command(str(i), {"created_at": now})
        self.assertLessEqual(len(self.bot.PENDING_COMMANDS), self.bot.MAX_CACHE_ENTRIES)
        self.bot.PENDING_COMMANDS["expired"] = {"created_at": now - 300}
        self.bot._prune_pending_commands()
        self.assertNotIn("expired", self.bot.PENDING_COMMANDS)

    def test_persisted_pending_state_is_bounded_and_expires(self):
        now = time.time()
        state = {
            str(i): {"created_at": now}
            for i in range(self.bot.MAX_CACHE_ENTRIES + 10)
        }
        state["expired"] = {"created_at": now - 300}
        self.assertTrue(self.bot._prune_pending_state(state))
        self.assertLessEqual(len(state), self.bot.MAX_CACHE_ENTRIES)
        self.assertNotIn("expired", state)

    def test_room_page_cache_is_bounded(self):
        for i in range(self.bot.MAX_CACHE_ENTRIES + 10):
            self.bot._remember_room_member_page(str(i), i)
        self.assertLessEqual(len(self.bot.ROOM_MEMBER_PAGES), self.bot.MAX_CACHE_ENTRIES)

    def test_welcome_membership_fallback_is_throttled(self):
        self.assertEqual(self.bot.WELCOME_FALLBACK_SCAN_SECONDS, 30.0)


if __name__ == "__main__":
    unittest.main()
