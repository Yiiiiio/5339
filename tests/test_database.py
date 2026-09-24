"""Integration checks for D; run from the project root after installing dependencies."""
import csv
import hashlib
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from build_database import build_database, load_spatial


class DatabaseTests(unittest.TestCase):
    def test_standalone_schema_and_foreign_key(self):
        with duckdb.connect() as con:
            load_spatial(con, ROOT)
            con.execute((ROOT / 'sql/schema.sql').read_text())
            tables = {r[0] for r in con.execute('SHOW TABLES').fetchall()}
            self.assertIn('charger_records', tables)
            self.assertIn('attribute_connectors', tables)
            with self.assertRaises(duckdb.ConstraintException):
                con.execute("INSERT INTO charger_attributes(record_id, external_source, external_station_id, match_decision) VALUES ('missing', 'test', '1', 'manual_accept')")
            con.execute("INSERT INTO operators VALUES ('test')")
            con.execute("INSERT INTO charger_records(record_id, Latitude, Longitude, operator_standardized) VALUES ('test', -33.8, 151.2, 'test')")
            for external_id in ('1', '2'):
                con.execute("INSERT INTO charger_attributes(record_id, external_source, external_station_id, match_decision) VALUES ('test', 'test', ?, 'manual_accept')", [external_id])
            con.execute("INSERT INTO charger_sa4(record_id, sa4_match_status) VALUES ('test', 'unmatched')")
            self.assertEqual(con.execute('SELECT count(*) FROM charger_overview').fetchone()[0], 1)
            self.assertTrue(con.execute('SELECT has_augmentation FROM charger_overview').fetchone()[0])
            with self.assertRaises(duckdb.ConstraintException):
                con.execute("INSERT INTO charger_records(record_id, Latitude, Longitude) VALUES ('invalid', 100, 151)")

    def invalid_input_case(self, filename, field, value):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shutil.copytree(ROOT / 'data/processed', root / 'data/processed')
            shutil.copytree(ROOT / 'sql', root / 'sql')
            (root / 'data/raw').mkdir()
            (root / 'data/raw/SA4_2026_AUST_SHP_GDA2020.zip').symlink_to(ROOT / 'data/raw/SA4_2026_AUST_SHP_GDA2020.zip')
            (root / 'data/final').mkdir()
            db = root / 'data/final/project.duckdb'
            db.write_bytes(b'previous successful database sentinel')
            original = hashlib.sha256(db.read_bytes()).digest()
            path = root / 'data/processed' / filename
            with path.open(encoding='utf-8-sig', newline='') as handle:
                reader = csv.DictReader(handle)
                fields, rows = reader.fieldnames, list(reader)
            rows[0][field] = value
            with path.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)
            with self.assertRaises(ValueError):
                build_database(root)
            self.assertEqual(hashlib.sha256(db.read_bytes()).digest(), original)

    def test_orphan_augmentation_rejected_without_overwriting(self):
        self.invalid_input_case('charger_attributes.csv', 'record_id', 'unknown_record')

    def test_stale_spatial_handoff_rejected_without_overwriting(self):
        self.invalid_input_case('charger_sa4.csv', 'Longitude', '0')


if __name__ == '__main__':
    unittest.main()
