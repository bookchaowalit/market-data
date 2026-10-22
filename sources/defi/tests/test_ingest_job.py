"""The ingest CLI as a scheduled job reports failures to Sentry (opt-in).

data-lake-pipelines' market capture runs ``python -m book_defi.ingest`` on a schedule. ``main``
catches every run failure, prints one line and returns 1 (or 2 for a lake failure), so an
unhandled-exception hook never sees it. These tests pin that a failed run, and a part of a
run that is logged and skipped, reach Sentry when ``SENTRY_DSN`` is set, and that Sentry is
never initialised without it. No network: providers, the lake and ``sentry_sdk`` are fakes.
"""

from __future__ import annotations

import io
import os
import sys
import types
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

from book_defi import ingest

SOURCE_ID = "book-defi-data"


class FakeSentry(types.ModuleType):
    def __init__(self) -> None:
        super().__init__("sentry_sdk")
        self.init_kwargs: dict | None = None
        self.events: list[tuple[str, str]] = []
        self.tags: dict[str, str] = {}

    def init(self, **kwargs):
        self.init_kwargs = kwargs

    def set_tag(self, key, value):
        self.tags[key] = value

    def capture_exception(self, exc=None):
        self.events.append(("error", type(exc).__name__))

    def capture_message(self, message, level=None):
        self.events.append((level or "info", message))

    def flush(self, timeout=None):
        return None


class IngestJobSentryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.sentry = FakeSentry()
        patcher = mock.patch.dict(sys.modules, {"sentry_sdk": self.sentry})
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_main(self, argv, dsn="https://public@example.invalid/1"):
        env = {k: v for k, v in os.environ.items() if k != "SENTRY_DSN"}
        if dsn:
            env["SENTRY_DSN"] = dsn
        with mock.patch.dict(os.environ, env, clear=True):
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                return ingest.main(argv)

    def errors(self):
        return [event for event in self.sentry.events if event[0] == "error"]

    def test_failed_run_reaches_sentry_once(self) -> None:
        with mock.patch.object(
            ingest, "run_live_ingest", side_effect=RuntimeError("upstream down")
        ):
            code = self.run_main(["--no-project-csv"])
        self.assertEqual(code, 1)
        self.assertEqual(len(self.errors()), 1)
        self.assertIsNotNone(self.sentry.init_kwargs)
        assert self.sentry.init_kwargs is not None
        self.assertFalse(self.sentry.init_kwargs["send_default_pii"])
        self.assertEqual(self.sentry.init_kwargs["traces_sample_rate"], 0.0)
        self.assertEqual(self.sentry.tags.get("source_id"), SOURCE_ID)
        # Security review: no frame locals, request bodies or raw exception messages.
        self.assertFalse(self.sentry.init_kwargs["include_local_variables"])
        self.assertEqual(self.sentry.init_kwargs["max_request_body_size"], "never")
        self.assertTrue(callable(self.sentry.init_kwargs["before_send"]))
        self.assertIn("RuntimeError", self.errors()[0][1])
        self.assertNotIn("upstream down", self.errors()[0][1])

    def test_before_send_strips_frame_vars_request_and_exception_text(self) -> None:
        with mock.patch.object(
            ingest, "run_live_ingest", side_effect=RuntimeError("x")
        ):
            self.run_main(["--no-project-csv"])
        assert self.sentry.init_kwargs is not None
        event = {
            "request": {"data": "token=secret"},
            "exception": {
                "values": [
                    {
                        "type": "HTTPError",
                        "value": "GET https://example.invalid/?key=secret failed",
                        "stacktrace": {
                            "frames": [{"function": "f", "vars": {"body": "secret"}}]
                        },
                    }
                ]
            },
        }
        scrubbed = self.sentry.init_kwargs["before_send"](event, {})
        self.assertNotIn("secret", repr(scrubbed))
        self.assertNotIn("request", scrubbed)
        self.assertEqual(scrubbed["exception"]["values"][0]["value"], "HTTPError")

    def test_lake_failure_exit_2_reaches_sentry_once(self) -> None:
        lake_error = ingest._lake.LakeIngestError("lake write failed")
        with mock.patch.object(ingest, "run_live_ingest", side_effect=lake_error):
            code = self.run_main(["--no-project-csv"])
        self.assertEqual(code, 2)
        self.assertEqual(len(self.errors()), 1)

    def test_successful_run_sends_nothing(self) -> None:
        with mock.patch.object(ingest, "run_live_ingest", return_value={}):
            code = self.run_main(["--no-project-csv"])
        self.assertEqual(code, 0)
        self.assertEqual(self.sentry.events, [])

    def test_without_dsn_sentry_stays_off(self) -> None:
        with mock.patch.object(
            ingest, "run_live_ingest", side_effect=RuntimeError("upstream down")
        ):
            code = self.run_main(["--no-project-csv"], dsn="")
        self.assertEqual(code, 1)
        self.assertIsNone(self.sentry.init_kwargs)
        self.assertEqual(self.sentry.events, [])


class CronRunnerSentryTest(unittest.TestCase):
    """scripts/run_defi.py is what setup_cron.sh schedules (daily 07:00 when installed)."""

    def setUp(self) -> None:
        import importlib.util
        from pathlib import Path

        self.sentry = FakeSentry()
        patcher = mock.patch.dict(sys.modules, {"sentry_sdk": self.sentry})
        patcher.start()
        self.addCleanup(patcher.stop)
        path = Path(__file__).resolve().parents[1] / "scripts" / "run_defi.py"
        spec = importlib.util.spec_from_file_location("run_defi_under_test", path)
        assert spec is not None and spec.loader is not None
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)

    def test_failed_cron_capture_reaches_sentry_and_exits_1(self) -> None:
        async def boom(output_dir):
            raise ValueError("DefiLlama capture produced only 0 pools")

        env = {k: v for k, v in os.environ.items() if k != "SENTRY_DSN"}
        env["SENTRY_DSN"] = "https://public@example.invalid/1"
        with (
            mock.patch.dict(os.environ, env, clear=True),
            mock.patch.object(self.module, "run_defi", side_effect=boom),
            mock.patch.object(sys, "argv", ["run_defi.py"]),
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
        ):
            code = self.module.main()
        self.assertEqual(code, 1)
        self.assertEqual(len(self.sentry.events), 1)
        level, message = self.sentry.events[0]
        self.assertEqual(level, "error")
        self.assertIn("ValueError", message)
        self.assertNotIn("0 pools", message)


if __name__ == "__main__":
    unittest.main()
