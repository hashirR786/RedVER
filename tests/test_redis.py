import unittest
import os
import time
import threading
import urllib.request
import json
import asyncio
from src.protocol import parse_resp, encode_response, SimpleString
from src.storage import StorageEngine
from src.persistence import PersistenceManager
from src.server import RedisServer

class TestProtocol(unittest.TestCase):
    def test_parse_simple_string(self):
        val, consumed = parse_resp(b"+OK\r\n")
        self.assertEqual(val, SimpleString("OK"))
        self.assertEqual(consumed, 5)

    def test_parse_error(self):
        val, consumed = parse_resp(b"-ERR unknown command\r\n")
        self.assertIsInstance(val, Exception)
        self.assertEqual(str(val), "ERR unknown command")
        self.assertEqual(consumed, 22)

    def test_parse_integer(self):
        val, consumed = parse_resp(b":1024\r\n")
        self.assertEqual(val, 1024)
        self.assertEqual(consumed, 7)

    def test_parse_bulk_string(self):
        val, consumed = parse_resp(b"$6\r\nfoobar\r\n")
        self.assertEqual(val, "foobar")
        self.assertEqual(consumed, 12)

    def test_parse_bulk_string_nil(self):
        val, consumed = parse_resp(b"$-1\r\n")
        self.assertIsNone(val)
        self.assertEqual(consumed, 5)

    def test_parse_array(self):
        val, consumed = parse_resp(b"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n")
        self.assertEqual(val, ["GET", "name"])
        self.assertEqual(consumed, 23)

    def test_parse_inline(self):
        val, consumed = parse_resp(b"SET key val\r\n")
        self.assertEqual(val, ["SET", "key", "val"])
        self.assertEqual(consumed, 13)

    def test_encode_responses(self):
        self.assertEqual(encode_response(None), b"$-1\r\n")
        self.assertEqual(encode_response(SimpleString("OK")), b"+OK\r\n")
        self.assertEqual(encode_response(123), b":123\r\n")
        self.assertEqual(encode_response("test"), b"$4\r\ntest\r\n")
        self.assertEqual(encode_response(["GET", "key"]), b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")


class TestStorageEngine(unittest.TestCase):
    def setUp(self):
        self.db = StorageEngine()

    def test_set_get(self):
        res = self.db.execute(["SET", "mykey", "myval"])
        self.assertEqual(res, SimpleString("OK"))
        self.assertEqual(self.db.execute(["GET", "mykey"]), "myval")

    def test_del_exists(self):
        self.db.execute(["SET", "k1", "v1"])
        self.db.execute(["SET", "k2", "v2"])
        
        self.assertEqual(self.db.execute(["EXISTS", "k1", "k2", "k3"]), 2)
        self.assertEqual(self.db.execute(["DEL", "k1", "k3"]), 1)
        self.assertEqual(self.db.execute(["EXISTS", "k1"]), 0)

    def test_ttl_expiry(self):
        self.db.execute(["SET", "tempkey", "tempval", "EX", "1"])
        self.assertEqual(self.db.execute(["EXISTS", "tempkey"]), 1)
        self.assertGreaterEqual(self.db.execute(["TTL", "tempkey"]), 0)
        
        time.sleep(1.2)
        # Should be passive evicted
        self.assertEqual(self.db.execute(["GET", "tempkey"]), None)
        self.assertEqual(self.db.execute(["TTL", "tempkey"]), -2)


class TestPersistence(unittest.TestCase):
    def setUp(self):
        self.aof_path = "tests/test_appendonly.aof"
        self.rdb_path = "tests/test_dump.rdb"
        # Ensure directories exist
        os.makedirs("tests", exist_ok=True)
        self._cleanup()
        
        self.db = StorageEngine()
        self.pm = PersistenceManager(self.db, aof_path=self.aof_path, rdb_path=self.rdb_path)

    def tearDown(self):
        self.pm.close()
        self._cleanup()

    def _cleanup(self):
        for path in [self.aof_path, self.rdb_path, f"{self.rdb_path}.tmp"]:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

    def test_aof_persistence(self):
        self.pm.start_aof()
        self.db.execute(["SET", "aofkey", "aofval"])
        self.db.execute(["SET", "k2", "v2"])
        self.db.execute(["DEL", "k2"])
        self.pm.close()

        # Create new engine and restore from AOF
        new_db = StorageEngine()
        new_pm = PersistenceManager(new_db, aof_path=self.aof_path, rdb_path=self.rdb_path)
        
        self.assertTrue(new_pm.load_aof())
        self.assertEqual(new_db.execute(["GET", "aofkey"]), "aofval")
        self.assertIsNone(new_db.execute(["GET", "k2"]))
        new_pm.close()

    def test_rdb_persistence(self):
        self.db.execute(["SET", "rdbkey", "rdbval"])
        self.db.execute(["SET", "expkey", "expval", "EX", "100"])
        
        self.assertTrue(self.pm.save_snapshot())
        self.assertTrue(os.path.exists(self.rdb_path))

        # Restore in a new engine
        new_db = StorageEngine()
        new_pm = PersistenceManager(new_db, aof_path=self.aof_path, rdb_path=self.rdb_path)
        
        self.assertTrue(new_pm.load_snapshot())
        self.assertEqual(new_db.execute(["GET", "rdbkey"]), "rdbval")
        self.assertGreater(new_db.execute(["TTL", "expkey"]), 50)
        new_pm.close()


class TestHTTPServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # We create a new event loop and run the RedisServer inside a separate daemon thread
        cls.loop = asyncio.new_event_loop()
        cls.server = RedisServer(host="127.0.0.1", port=6389, http_port=8089, aof_enabled=False)
        
        def run_server():
            asyncio.set_event_loop(cls.loop)
            cls.loop.run_until_complete(cls.server.start())
            
        cls.thread = threading.Thread(target=run_server, daemon=True)
        cls.thread.start()
        # Give server time to spin up and bind ports
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        # Gracefully stop servers and stop loop
        if cls.server.is_running:
            cls.loop.call_soon_threadsafe(cls.server.stop)
        time.sleep(0.3)
        cls.loop.call_soon_threadsafe(cls.loop.stop)
        cls.thread.join(timeout=2.0)

    def test_http_index(self):
        url = "http://127.0.0.1:8089/"
        with urllib.request.urlopen(url) as response:
            html = response.read().decode('utf-8')
            self.assertIn("RedVER", html)
            self.assertIn("Keyspace Browser", html)

    def test_http_api_stats(self):
        url = "http://127.0.0.1:8089/api/stats"
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode('utf-8'))
            self.assertIn("uptime", data)
            self.assertIn("ops", data)
            self.assertGreaterEqual(data.get("keys_count"), 0)

    def test_http_api_exec_and_keys(self):
        # Execute SET key
        url = "http://127.0.0.1:8089/api/exec"
        req_data = json.dumps({"cmd": "SET test_http_key test_http_val"}).encode('utf-8')
        req = urllib.request.Request(url, data=req_data, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
            self.assertEqual(data.get("status"), "success")
            self.assertEqual(data.get("result"), "OK")

        # Query stats again
        stats_url = "http://127.0.0.1:8089/api/stats"
        with urllib.request.urlopen(stats_url) as response:
            data = json.loads(response.read().decode('utf-8'))
            self.assertEqual(data.get("keys_count"), 1)

        # Retrieve keys list
        keys_url = "http://127.0.0.1:8089/api/keys"
        with urllib.request.urlopen(keys_url) as response:
            keys_data = json.loads(response.read().decode('utf-8'))
            keys_list = keys_data.get("keys", [])
            names = [k.get("name") for k in keys_list]
            values = [k.get("value") for k in keys_list]
            self.assertIn("test_http_key", names)
            self.assertIn("test_http_val", values)


if __name__ == "__main__":
    unittest.main()
