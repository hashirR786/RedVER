import unittest
import os
import time
import threading
import urllib.request
import json
import asyncio
from src.protocol import parse_resp, encode_response, SimpleString, RespMap, RespDouble
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

class TestProtocol2(unittest.TestCase):
    """Tests for RESP3 types, inline quoted parsing, and pipelining."""

    # ── RESP3 encoder ──────────────────────────────────────────────────────
    def test_resp3_null(self):
        self.assertEqual(encode_response(None, resp3=True), b"_\r\n")

    def test_resp3_boolean(self):
        self.assertEqual(encode_response(True,  resp3=True), b"#t\r\n")
        self.assertEqual(encode_response(False, resp3=True), b"#f\r\n")
        # RESP2 booleans stay as integers
        self.assertEqual(encode_response(True,  resp3=False), b":1\r\n")
        self.assertEqual(encode_response(False, resp3=False), b":0\r\n")

    def test_resp3_double(self):
        self.assertEqual(encode_response(RespDouble(3.14), resp3=True), b",3.14\r\n")
        self.assertEqual(encode_response(RespDouble(float('inf')),  resp3=True), b",inf\r\n")
        self.assertEqual(encode_response(RespDouble(float('-inf')), resp3=True), b",-inf\r\n")

    def test_resp3_map_encode(self):
        wire = encode_response(RespMap({"a": 1}), resp3=True)
        self.assertEqual(wire, b"%1\r\n$1\r\na\r\n:1\r\n")

    def test_resp3_map_encode_resp2_fallback(self):
        # In RESP2 mode a RespMap should not occur, but a plain dict flattens to array
        wire = encode_response({"x": "y"}, resp3=False)
        self.assertEqual(wire, b"*2\r\n$1\r\nx\r\n$1\r\ny\r\n")

    # ── RESP3 parser ───────────────────────────────────────────────────────
    def test_parse_resp3_null(self):
        val, consumed = parse_resp(b"_\r\n")
        self.assertIsNone(val)
        self.assertEqual(consumed, 3)

    def test_parse_resp3_boolean(self):
        t, _ = parse_resp(b"#t\r\n")
        f, _ = parse_resp(b"#f\r\n")
        self.assertTrue(t)
        self.assertFalse(f)

    def test_parse_resp3_double(self):
        val, consumed = parse_resp(b",3.14\r\n")
        self.assertIsInstance(val, RespDouble)
        self.assertAlmostEqual(val.value, 3.14)
        self.assertEqual(consumed, 7)

    def test_parse_resp3_map(self):
        # %1\r\n + key + value
        wire = b"%1\r\n$3\r\nfoo\r\n:42\r\n"
        val, consumed = parse_resp(wire)
        self.assertIsInstance(val, RespMap)
        self.assertEqual(val.data["foo"], 42)
        self.assertEqual(consumed, len(wire))

    def test_parse_resp3_big_number(self):
        val, consumed = parse_resp(b"(9999999999999999\r\n")
        self.assertEqual(val, 9999999999999999)

    # ── Inline quoted tokens ───────────────────────────────────────────────
    def test_inline_quoted_value(self):
        """Inline SET with a quoted multi-word value must parse as 3 tokens."""
        val, consumed = parse_resp(b'SET mykey "hello world"\r\n')
        self.assertEqual(val, ["SET", "mykey", "hello world"])
        self.assertEqual(consumed, 25)

    def test_inline_lf_only(self):
        """\\n-only terminator (netcat default) must be accepted."""
        val, consumed = parse_resp(b"PING\n")
        self.assertEqual(val, ["PING"])
        self.assertEqual(consumed, 5)

    def test_inline_blank_line(self):
        """A blank line must return an empty list (skipped by server)."""
        val, consumed = parse_resp(b"\r\n")
        self.assertEqual(val, [])
        self.assertEqual(consumed, 2)

    # ── Pipelining ─────────────────────────────────────────────────────────
    def test_pipelining_two_commands_in_one_buffer(self):
        """
        Two complete RESP messages concatenated in one TCP chunk must both
        parse correctly without either being dropped.
        """
        cmd1 = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
        cmd2 = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n"
        buf  = cmd1 + cmd2

        val1, c1 = parse_resp(buf)
        self.assertEqual(val1, ["SET", "a", "1"])

        val2, c2 = parse_resp(buf[c1:])
        self.assertEqual(val2, ["GET", "a"])

        self.assertEqual(c1 + c2, len(buf))   # all bytes consumed

    def test_pipelining_partial_tail_returns_zero(self):
        """
        A complete first message + incomplete second message must consume
        exactly the first message and return 0 for the second (caller buffers).
        """
        complete = b"*1\r\n$4\r\nPING\r\n"
        partial  = b"*2\r\n$3\r\nGET\r\n"   # missing the key bulk string
        buf = complete + partial

        val1, c1 = parse_resp(buf)
        self.assertEqual(val1, ["PING"])

        val2, c2 = parse_resp(buf[c1:])
        self.assertIsNone(val2)               # incomplete → wait for more
        self.assertEqual(c2, 0)



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

    def test_save_command(self):
        self.db.execute(["SET", "cmdkey", "cmdval"])
        res = self.db.execute(["SAVE"])
        self.assertEqual(res, SimpleString("OK"))
        self.assertTrue(os.path.exists(self.rdb_path))



class TestHTTPServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # We create a new event loop and run the RedisServer inside a separate daemon thread
        cls.loop = asyncio.new_event_loop()
        cls.server = RedisServer(host="127.0.0.1", port=6389, http_port=8089, aof_enabled=False)
        cls.server.persistence.rdb_path = "tests/test_http_dump.rdb"
        cls.server.persistence.aof_path = "tests/test_http_appendonly.aof"
        cls.server.storage.cmd_flushdb()
        
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



class TestDataStructures(unittest.TestCase):
    """Tests for List, Hash, Set, Sorted Set data structures and TYPE command."""

    def setUp(self):
        self.db = StorageEngine()

    # ── List ──────────────────────────────────────────────────────────────
    def test_list_lpush_rpush_lrange(self):
        self.assertEqual(self.db.execute(["RPUSH", "lst", "a", "b", "c"]), 3)
        self.assertEqual(self.db.execute(["LPUSH", "lst", "z"]), 4)
        self.assertEqual(self.db.execute(["LRANGE", "lst", "0", "-1"]), ["z", "a", "b", "c"])

    def test_list_lpop_rpop(self):
        self.db.execute(["RPUSH", "lst2", "x", "y", "z"])
        self.assertEqual(self.db.execute(["LPOP", "lst2"]), "x")
        self.assertEqual(self.db.execute(["RPOP", "lst2"]), "z")
        self.assertEqual(self.db.execute(["LLEN", "lst2"]), 1)

    def test_list_llen_empty(self):
        self.assertEqual(self.db.execute(["LLEN", "nokey"]), 0)

    def test_list_lrange_negative(self):
        self.db.execute(["RPUSH", "rl", "a", "b", "c", "d"])
        self.assertEqual(self.db.execute(["LRANGE", "rl", "-2", "-1"]), ["c", "d"])

    def test_list_pop_deletes_empty_key(self):
        self.db.execute(["RPUSH", "one", "only"])
        self.db.execute(["LPOP", "one"])
        self.assertIsNone(self.db.execute(["GET", "one"]))  # key gone

    def test_list_type_guard(self):
        self.db.execute(["SET", "strkey", "val"])
        res = self.db.execute(["LPUSH", "strkey", "x"])
        self.assertIsInstance(res, Exception)
        self.assertIn("WRONGTYPE", str(res))

    # ── Hash ──────────────────────────────────────────────────────────────
    def test_hash_hset_hget(self):
        self.assertEqual(self.db.execute(["HSET", "user", "name", "hashir"]), 1)
        self.assertEqual(self.db.execute(["HSET", "user", "age", "21"]), 1)
        self.assertEqual(self.db.execute(["HGET", "user", "name"]), "hashir")
        self.assertIsNone(self.db.execute(["HGET", "user", "missing"]))

    def test_hash_hset_update(self):
        self.db.execute(["HSET", "h", "f", "v1"])
        result = self.db.execute(["HSET", "h", "f", "v2"])  # update
        self.assertEqual(result, 0)
        self.assertEqual(self.db.execute(["HGET", "h", "f"]), "v2")

    def test_hash_hgetall(self):
        self.db.execute(["HSET", "h2", "a", "1"])
        self.db.execute(["HSET", "h2", "b", "2"])
        flat = self.db.execute(["HGETALL", "h2"])
        self.assertEqual(set(zip(flat[::2], flat[1::2])), {("a", "1"), ("b", "2")})

    def test_hash_hdel_hkeys_hlen(self):
        self.db.execute(["HSET", "h3", "x", "1"])
        self.db.execute(["HSET", "h3", "y", "2"])
        self.assertEqual(self.db.execute(["HLEN", "h3"]), 2)
        self.db.execute(["HDEL", "h3", "x"])
        self.assertEqual(self.db.execute(["HKEYS", "h3"]), ["y"])

    def test_hash_type_guard(self):
        self.db.execute(["SET", "strkey2", "val"])
        res = self.db.execute(["HSET", "strkey2", "f", "v"])
        self.assertIsInstance(res, Exception)
        self.assertIn("WRONGTYPE", str(res))

    # ── Set ───────────────────────────────────────────────────────────────
    def test_set_sadd_smembers(self):
        self.assertEqual(self.db.execute(["SADD", "s1", "a", "b", "c"]), 3)
        self.assertEqual(self.db.execute(["SADD", "s1", "a"]), 0)  # duplicate
        members = self.db.execute(["SMEMBERS", "s1"])
        self.assertEqual(set(members), {"a", "b", "c"})

    def test_set_srem_sismember(self):
        self.db.execute(["SADD", "s2", "x", "y"])
        self.assertEqual(self.db.execute(["SISMEMBER", "s2", "x"]), 1)
        self.db.execute(["SREM", "s2", "x"])
        self.assertEqual(self.db.execute(["SISMEMBER", "s2", "x"]), 0)

    def test_set_sunion_sinter_sdiff(self):
        self.db.execute(["SADD", "sa", "1", "2", "3"])
        self.db.execute(["SADD", "sb", "2", "3", "4"])
        self.assertEqual(set(self.db.execute(["SUNION", "sa", "sb"])), {"1", "2", "3", "4"})
        self.assertEqual(set(self.db.execute(["SINTER", "sa", "sb"])), {"2", "3"})
        self.assertEqual(set(self.db.execute(["SDIFF", "sa", "sb"])), {"1"})

    def test_set_type_guard(self):
        self.db.execute(["SET", "strkey3", "val"])
        res = self.db.execute(["SADD", "strkey3", "m"])
        self.assertIsInstance(res, Exception)
        self.assertIn("WRONGTYPE", str(res))

    # ── Sorted Set ────────────────────────────────────────────────────────
    def test_zset_zadd_zrange_zscore(self):
        self.db.execute(["ZADD", "lb", "100", "alice"])
        self.db.execute(["ZADD", "lb", "200", "bob"])
        self.db.execute(["ZADD", "lb", "50", "charlie"])
        self.assertEqual(self.db.execute(["ZRANGE", "lb", "0", "-1"]),
                         ["charlie", "alice", "bob"])
        self.assertEqual(self.db.execute(["ZSCORE", "lb", "bob"]), "200.0")

    def test_zset_zrank(self):
        self.db.execute(["ZADD", "lb2", "10", "a"])
        self.db.execute(["ZADD", "lb2", "20", "b"])
        self.assertEqual(self.db.execute(["ZRANK", "lb2", "a"]), 0)
        self.assertEqual(self.db.execute(["ZRANK", "lb2", "b"]), 1)
        self.assertIsNone(self.db.execute(["ZRANK", "lb2", "missing"]))

    def test_zset_zrem(self):
        self.db.execute(["ZADD", "lb3", "5", "x"])
        self.db.execute(["ZADD", "lb3", "10", "y"])
        self.assertEqual(self.db.execute(["ZREM", "lb3", "x"]), 1)
        self.assertEqual(self.db.execute(["ZRANGE", "lb3", "0", "-1"]), ["y"])

    def test_zset_update_score(self):
        self.db.execute(["ZADD", "lb4", "1", "m"])
        self.assertEqual(self.db.execute(["ZADD", "lb4", "99", "m"]), 0)  # update
        self.assertEqual(self.db.execute(["ZSCORE", "lb4", "m"]), "99.0")
        self.assertEqual(self.db.execute(["ZRANK", "lb4", "m"]), 0)

    def test_zset_type_guard(self):
        self.db.execute(["SET", "strkey4", "val"])
        res = self.db.execute(["ZADD", "strkey4", "1", "m"])
        self.assertIsInstance(res, Exception)
        self.assertIn("WRONGTYPE", str(res))

    # ── TYPE command ──────────────────────────────────────────────────────
    def test_type_command(self):
        self.db.execute(["SET", "sk", "v"])
        self.db.execute(["LPUSH", "lk", "v"])
        self.db.execute(["HSET", "hk", "f", "v"])
        self.db.execute(["SADD", "setk", "v"])
        self.db.execute(["ZADD", "zk", "1", "v"])
        self.assertEqual(self.db.execute(["TYPE", "sk"]),   SimpleString("string"))
        self.assertEqual(self.db.execute(["TYPE", "lk"]),   SimpleString("list"))
        self.assertEqual(self.db.execute(["TYPE", "hk"]),   SimpleString("hash"))
        self.assertEqual(self.db.execute(["TYPE", "setk"]), SimpleString("set"))
        self.assertEqual(self.db.execute(["TYPE", "zk"]),   SimpleString("zset"))
        self.assertEqual(self.db.execute(["TYPE", "none"]), SimpleString("none"))

    # ── RDB round-trip with new types ─────────────────────────────────────
    def test_rdb_roundtrip_all_types(self):
        aof_path = "tests/test_ds_aof.aof"
        rdb_path = "tests/test_ds_dump.rdb"
        for p in [aof_path, rdb_path]:
            if os.path.exists(p):
                os.remove(p)

        pm = PersistenceManager(self.db, aof_path=aof_path, rdb_path=rdb_path)
        self.db.execute(["SET",   "sk",   "hello"])
        self.db.execute(["RPUSH", "lk",   "a", "b"])
        self.db.execute(["HSET",  "hk",   "f", "v"])
        self.db.execute(["SADD",  "setk", "x"])
        self.db.execute(["ZADD",  "zk",   "7", "m"])
        self.assertTrue(pm.save_snapshot())
        pm.close()

        new_db = StorageEngine()
        new_pm = PersistenceManager(new_db, aof_path=aof_path, rdb_path=rdb_path)
        self.assertTrue(new_pm.load_snapshot())

        self.assertEqual(new_db.execute(["GET",       "sk"]),        "hello")
        self.assertEqual(new_db.execute(["LRANGE",    "lk", "0", "-1"]), ["a", "b"])
        self.assertEqual(new_db.execute(["HGET",      "hk", "f"]),   "v")
        self.assertEqual(new_db.execute(["SMEMBERS",  "setk"]),      ["x"])
        self.assertEqual(new_db.execute(["ZSCORE",    "zk",  "m"]),  "7.0")
        new_pm.close()
        for p in [aof_path, rdb_path]:
            if os.path.exists(p):
                os.remove(p)


if __name__ == "__main__":
    unittest.main()
