"""Regression tests for persistent PTR cache and bounded scheduling schema."""
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from flow_collector import connect_db


class PTRStorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "flows.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_dns_due_index_created(self):
        db = connect_db(self.path)
        indexes = {row[1] for row in db.execute("PRAGMA index_list(dns_jobs)")}
        self.assertIn("dns_jobs_due", indexes)
        db.close()

    def test_dns_cache_survives_reconnect_and_supports_negative_entries(self):
        db = connect_db(self.path)
        db.execute("INSERT INTO dns_cache(ip,hostname,expires_at) VALUES(?,?,?)",
                   ("192.0.2.1", None, 9999999999))
        db.execute("INSERT INTO dns_cache(ip,hostname,expires_at) VALUES(?,?,?)",
                   ("192.0.2.2", "example.test", 9999999999))
        db.close()
        db = connect_db(self.path)
        self.assertEqual(db.execute("SELECT hostname FROM dns_cache WHERE ip='192.0.2.1'").fetchone(), (None,))
        self.assertEqual(db.execute("SELECT hostname FROM dns_cache WHERE ip='192.0.2.2'").fetchone(), ("example.test",))
        db.close()

    def test_existing_legacy_jobs_are_not_deleted(self):
        db = sqlite3.connect(self.path)
        db.execute("CREATE TABLE dns_jobs(router_name TEXT NOT NULL,connection_id TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(router_name,connection_id))")
        db.execute("INSERT INTO dns_jobs VALUES('router','conn','{}')")
        db.commit()
        db.close()
        db = connect_db(self.path)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM dns_jobs").fetchone()[0], 1)
        self.assertIn("next_attempt", [row[1] for row in db.execute("PRAGMA table_info(dns_jobs)")])
        db.close()


if __name__ == "__main__":
    unittest.main()
