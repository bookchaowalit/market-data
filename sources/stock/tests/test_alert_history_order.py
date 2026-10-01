"""'Since last check' alerts must compare against history *before* this run."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from book_stock import ingest, lake

QUOTE = {
    "symbol": "AAPL",
    "price": 190.0,
    "prev_close": 189.0,
    "change": 1.0,
    "change_pct": 0.53,
    "currency": "USD",
    "exchange": "NMS",
    "timestamp": "2026-08-01 16:00:00",
}


def _fake_ingest(**kwargs):
    return {
        "status": "success",
        "run_id": "test-run",
        "record_count": len(kwargs["records"]),
        "raw_key": "landing/test",
        "bronze_key": "bronze/test",
        "manifest_key": "control/test",
        "data_lake": {"uri": "file:///tmp/lake"},
    }


@unittest.skipUnless(
    lake.shared_runtime_available(),
    "shared data_lake runtime not installed (pip install -e .[lake])",
)
class AlertHistoryOrderTests(unittest.TestCase):
    def test_multi_run_move_is_alerted(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            # A previous run recorded AAPL at 100; this run sees 190 (+90%).
            ingest.project_history_csv([{**QUOTE, "price": 100.0}], out)
            landing = json.dumps({"responses": {"AAPL": "{}"}}).encode("utf-8")
            buf = io.StringIO()
            with mock.patch.object(lake, "ingest_to_lake", side_effect=_fake_ingest), mock.patch.object(
                lake, "write_lineage", return_value=out / "lake_lineage.json"
            ), mock.patch.object(
                ingest, "fetch_quotes_raw", return_value=(landing, [dict(QUOTE)], {})
            ), redirect_stdout(buf):
                ingest.run_live_ingest(
                    symbols=["AAPL"], output_dir=out, alert_threshold=3.0,
                    write_history=True, project_csv=True,
                )
            self.assertIn("RISING", buf.getvalue())
            self.assertIn("since last check", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
