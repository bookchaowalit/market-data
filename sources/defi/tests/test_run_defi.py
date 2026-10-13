import unittest
from pathlib import Path


class RunDefiTests(unittest.TestCase):
    def test_collection_scripts_exist(self):
        root = Path(__file__).resolve().parents[1]
        self.assertTrue((root / "scripts" / "run_defi.py").is_file())
        self.assertTrue((root / "scripts" / "scrape_defi_yields.py").is_file())
        self.assertTrue((root / "setup_cron.sh").is_file())


if __name__ == "__main__":
    unittest.main()
