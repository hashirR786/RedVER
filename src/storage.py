import time
import fnmatch
from collections import deque
from sortedcontainers import SortedList
from src.protocol import SimpleString

# Type name constants (match Redis type strings)
TYPE_STRING = "string"
TYPE_LIST   = "list"
TYPE_HASH   = "hash"
TYPE_SET    = "set"
TYPE_ZSET   = "zset"


class StorageEngine:
    """
    In-memory database engine for RedVER.
    Supports String, List, Hash, Set, and Sorted Set data types.
    Handles TTL/expiration, type tagging, and hooks for AOF logging.
    """
    def __init__(self):
        self._db = {}       # key (str) -> value (str | deque | dict | set | SortedList)
        self._types = {}    # key (str) -> type string
        self._expires = {}  # key (str) -> expiration timestamp (float)
        self._aof_callback = None
        self._save_callback = None

    def set_aof_callback(self, callback):
        """Sets the write callback function that intercepts modifying commands."""
        self._aof_callback = callback

    def _log_write(self, cmd_args: list):
        """Helper to invoke the write callback for logging to Append-Only File."""
        if self._aof_callback:
            self._aof_callback(cmd_args)

    def _is_expired(self, key: str) -> bool:
        """
        Checks if a key has expired and performs passive eviction.
        Returns True if expired, False otherwise.
        """
        if key not in self._expires:
            return False
        if time.time() > self._expires[key]:
            # Evict key and its metadata
            self._db.pop(key, None)
            self._expires.pop(key, None)
            self._types.pop(key, None)
            return True
        return False

    def _assert_type(self, key: str, expected: str):
        """
        Returns an Exception if the key exists but has a different type.
        Returns None if the key is absent or has the expected type.
        """
        actual = self._types.get(key)
        if actual is not None and actual != expected:
            return Exception(
                f"WRONGTYPE Operation against a key holding the wrong kind of value "
                f"(expected {expected}, got {actual})"
            )
        return None

    def _delete_key(self, key: str):
        """Internal helper: removes a key and all its metadata."""
        self._db.pop(key, None)
        self._types.pop(key, None)
        self._expires.pop(key, None)

    def execute(self, cmd_args: list) -> any:
        """
        Executes a Redis-compatible command and returns the result (or Exception).
        Arguments are parsed from RESP array.
        """
        if not cmd_args or not isinstance(cmd_args, list):
            return Exception("ERR empty or invalid command format")

        cmd = str(cmd_args[0]).upper()
        args = cmd_args[1:]

        method_name = f"cmd_{cmd.lower()}"
        if not hasattr(self, method_name):
            return Exception(f"ERR unknown command '{cmd}'")

        try:
            return getattr(self, method_name)(*args)
        except TypeError:
            return Exception(f"ERR wrong number of arguments for '{cmd.lower()}' command")
        except Exception as e:
            return Exception(f"ERR {str(e)}")

    # ─────────────────────────────────────────────
    # Core / String commands
    # ─────────────────────────────────────────────

    def cmd_ping(self, *args):
        if not args:
            return SimpleString("PONG")
        return args[0]

    def cmd_set(self, key: str, value: str, *options):
        """SET key value [EX seconds]"""
        expiry_sec = None
        if options:
            if len(options) == 2 and str(options[0]).upper() == "EX":
                try:
                    expiry_sec = int(options[1])
                except ValueError:
                    return Exception("ERR value is not an integer or out of range")
            else:
                return Exception("ERR syntax error")

        # Overwriting any existing type is allowed for SET
        self._db[key] = str(value)
        self._types[key] = TYPE_STRING
        if expiry_sec is not None:
            self._expires[key] = time.time() + expiry_sec
            self._log_write(["SET", key, str(value), "EX", str(expiry_sec)])
        else:
            self._expires.pop(key, None)
            self._log_write(["SET", key, str(value)])

        return SimpleString("OK")

    def cmd_get(self, key: str):
        if self._is_expired(key):
            return None
        err = self._assert_type(key, TYPE_STRING)
        if err:
            return err
        return self._db.get(key, None)

    def cmd_hello(self, *args):
        return [
            "server", "redis",
            "version", "6.0.0",
            "proto", 2,
            "id", 1,
            "mode", "standalone",
            "role", "master",
            "modules", []
        ]

    def cmd_incr(self, key: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_STRING)
        if err:
            return err
        val_str = self._db.get(key, "0")
        try:
            val_int = int(val_str)
        except ValueError:
            return Exception("ERR value is not an integer or out of range")

        new_val = val_int + 1
        self._db[key] = str(new_val)
        self._types[key] = TYPE_STRING
        self._log_write(["SET", key, str(new_val)])
        return new_val

    def cmd_del(self, *keys):
        if not keys:
            return Exception("ERR wrong number of arguments for 'del' command")
        deleted = 0
        log_keys = []
        for key in keys:
            self._is_expired(key)
            if key in self._db:
                self._delete_key(key)
                deleted += 1
                log_keys.append(key)
        if deleted > 0:
            self._log_write(["DEL", *log_keys])
        return deleted

    def cmd_exists(self, *keys):
        if not keys:
            return Exception("ERR wrong number of arguments for 'exists' command")
        count = 0
        for key in keys:
            if not self._is_expired(key) and key in self._db:
                count += 1
        return count

    def cmd_type(self, key: str):
        if self._is_expired(key) or key not in self._db:
            return SimpleString("none")
        return SimpleString(self._types.get(key, "string"))

    def cmd_expire(self, key: str, seconds: str):
        try:
            sec = int(seconds)
        except ValueError:
            return Exception("ERR value is not an integer or out of range")

        if self._is_expired(key) or key not in self._db:
            return 0

        self._expires[key] = time.time() + sec
        self._log_write(["EXPIRE", key, str(sec)])
        return 1

    def cmd_ttl(self, key: str):
        if self._is_expired(key) or key not in self._db:
            return -2
        if key not in self._expires:
            return -1
        remaining = int(self._expires[key] - time.time())
        return max(remaining, 0)

    def cmd_keys(self, pattern: str = "*"):
        all_keys = list(self._db.keys())
        for key in all_keys:
            self._is_expired(key)
        return [k for k in self._db if fnmatch.fnmatch(k, pattern)]

    def cmd_flushdb(self):
        self._db.clear()
        self._types.clear()
        self._expires.clear()
        self._log_write(["FLUSHDB"])
        return SimpleString("OK")

    def cmd_save(self):
        if self._save_callback:
            success = self._save_callback()
            if success:
                return SimpleString("OK")
            else:
                return Exception("ERR failed to save snapshot")
        return Exception("ERR snapshot save callback not registered")

    # ─────────────────────────────────────────────
    # List commands  (backed by collections.deque)
    # ─────────────────────────────────────────────

    def _get_or_create_list(self, key: str):
        """Returns the deque for key, creating it if absent. Returns Exception on type mismatch."""
        self._is_expired(key)
        err = self._assert_type(key, TYPE_LIST)
        if err:
            return err
        if key not in self._db:
            self._db[key] = deque()
            self._types[key] = TYPE_LIST
        return self._db[key]

    def cmd_lpush(self, key: str, *values):
        lst = self._get_or_create_list(key)
        if isinstance(lst, Exception):
            return lst
        for v in values:
            lst.appendleft(str(v))
        self._log_write(["LPUSH", key, *[str(v) for v in values]])
        return len(lst)

    def cmd_rpush(self, key: str, *values):
        lst = self._get_or_create_list(key)
        if isinstance(lst, Exception):
            return lst
        for v in values:
            lst.append(str(v))
        self._log_write(["RPUSH", key, *[str(v) for v in values]])
        return len(lst)

    def cmd_lpop(self, key: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_LIST)
        if err:
            return err
        lst = self._db.get(key)
        if not lst:
            return None
        val = lst.popleft()
        if not lst:
            self._delete_key(key)
        self._log_write(["LPOP", key])
        return val

    def cmd_rpop(self, key: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_LIST)
        if err:
            return err
        lst = self._db.get(key)
        if not lst:
            return None
        val = lst.pop()
        if not lst:
            self._delete_key(key)
        self._log_write(["RPOP", key])
        return val

    def cmd_lrange(self, key: str, start: str, stop: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_LIST)
        if err:
            return err
        lst = self._db.get(key)
        if not lst:
            return []
        try:
            start, stop = int(start), int(stop)
        except ValueError:
            return Exception("ERR value is not an integer or out of range")
        items = list(lst)
        length = len(items)
        # Normalize negative indices like Redis
        if start < 0:
            start = max(length + start, 0)
        if stop < 0:
            stop = length + stop
        stop = min(stop, length - 1)
        if start > stop:
            return []
        return items[start:stop + 1]

    def cmd_llen(self, key: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_LIST)
        if err:
            return err
        lst = self._db.get(key)
        return len(lst) if lst else 0

    # ─────────────────────────────────────────────
    # Hash commands  (backed by nested dict)
    # ─────────────────────────────────────────────

    def _get_or_create_hash(self, key: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_HASH)
        if err:
            return err
        if key not in self._db:
            self._db[key] = {}
            self._types[key] = TYPE_HASH
        return self._db[key]

    def cmd_hset(self, key: str, field: str, value: str):
        h = self._get_or_create_hash(key)
        if isinstance(h, Exception):
            return h
        is_new = field not in h
        h[field] = str(value)
        self._log_write(["HSET", key, field, str(value)])
        return 1 if is_new else 0

    def cmd_hget(self, key: str, field: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_HASH)
        if err:
            return err
        h = self._db.get(key, {})
        return h.get(field, None)

    def cmd_hgetall(self, key: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_HASH)
        if err:
            return err
        h = self._db.get(key, {})
        result = []
        for f, v in h.items():
            result.append(f)
            result.append(v)
        return result

    def cmd_hdel(self, key: str, *fields):
        if not fields:
            return Exception("ERR wrong number of arguments for 'hdel' command")
        self._is_expired(key)
        err = self._assert_type(key, TYPE_HASH)
        if err:
            return err
        h = self._db.get(key, {})
        deleted = 0
        for f in fields:
            if f in h:
                del h[f]
                deleted += 1
        if not h:
            self._delete_key(key)
        if deleted > 0:
            self._log_write(["HDEL", key, *fields])
        return deleted

    def cmd_hkeys(self, key: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_HASH)
        if err:
            return err
        return list(self._db.get(key, {}).keys())

    def cmd_hlen(self, key: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_HASH)
        if err:
            return err
        return len(self._db.get(key, {}))

    # ─────────────────────────────────────────────
    # Set commands  (backed by Python set)
    # ─────────────────────────────────────────────

    def _get_or_create_set(self, key: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_SET)
        if err:
            return err
        if key not in self._db:
            self._db[key] = set()
            self._types[key] = TYPE_SET
        return self._db[key]

    def cmd_sadd(self, key: str, *members):
        s = self._get_or_create_set(key)
        if isinstance(s, Exception):
            return s
        added = 0
        for m in members:
            if m not in s:
                s.add(str(m))
                added += 1
        if added > 0:
            self._log_write(["SADD", key, *[str(m) for m in members]])
        return added

    def cmd_srem(self, key: str, *members):
        if not members:
            return Exception("ERR wrong number of arguments for 'srem' command")
        self._is_expired(key)
        err = self._assert_type(key, TYPE_SET)
        if err:
            return err
        s = self._db.get(key, set())
        removed = 0
        for m in members:
            if m in s:
                s.remove(m)
                removed += 1
        if not s:
            self._delete_key(key)
        if removed > 0:
            self._log_write(["SREM", key, *[str(m) for m in members]])
        return removed

    def cmd_smembers(self, key: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_SET)
        if err:
            return err
        return list(self._db.get(key, set()))

    def cmd_sismember(self, key: str, member: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_SET)
        if err:
            return err
        return 1 if member in self._db.get(key, set()) else 0

    def cmd_sunion(self, *keys):
        result = set()
        for key in keys:
            self._is_expired(key)
            err = self._assert_type(key, TYPE_SET)
            if err:
                return err
            result |= self._db.get(key, set())
        return list(result)

    def cmd_sinter(self, *keys):
        if not keys:
            return []
        sets = []
        for key in keys:
            self._is_expired(key)
            err = self._assert_type(key, TYPE_SET)
            if err:
                return err
            sets.append(self._db.get(key, set()))
        result = sets[0].copy()
        for s in sets[1:]:
            result &= s
        return list(result)

    def cmd_sdiff(self, *keys):
        if not keys:
            return []
        self._is_expired(keys[0])
        err = self._assert_type(keys[0], TYPE_SET)
        if err:
            return err
        result = self._db.get(keys[0], set()).copy()
        for key in keys[1:]:
            self._is_expired(key)
            err = self._assert_type(key, TYPE_SET)
            if err:
                return err
            result -= self._db.get(key, set())
        return list(result)

    # ─────────────────────────────────────────────
    # Sorted Set commands  (backed by SortedList of (score, member) tuples)
    # ─────────────────────────────────────────────

    def _get_or_create_zset(self, key: str):
        """
        Returns (SortedList, scores_dict) for key.
        SortedList stores (score, member) tuples sorted by score then member.
        scores_dict maps member -> score for O(1) lookup.
        The value stored in _db is a tuple: (SortedList, dict).
        """
        self._is_expired(key)
        err = self._assert_type(key, TYPE_ZSET)
        if err:
            return err
        if key not in self._db:
            self._db[key] = (SortedList(key=lambda x: (x[0], x[1])), {})
            self._types[key] = TYPE_ZSET
        return self._db[key]

    def cmd_zadd(self, key: str, score: str, member: str):
        zset = self._get_or_create_zset(key)
        if isinstance(zset, Exception):
            return zset
        sl, scores = zset
        try:
            score_f = float(score)
        except ValueError:
            return Exception("ERR value is not a valid float")
        is_new = member not in scores
        if not is_new:
            # Remove old entry
            sl.remove((scores[member], member))
        sl.add((score_f, member))
        scores[member] = score_f
        self._log_write(["ZADD", key, str(score_f), member])
        return 1 if is_new else 0

    def cmd_zrange(self, key: str, start: str, stop: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_ZSET)
        if err:
            return err
        data = self._db.get(key)
        if not data:
            return []
        sl, _ = data
        try:
            start, stop = int(start), int(stop)
        except ValueError:
            return Exception("ERR value is not an integer or out of range")
        items = list(sl)
        length = len(items)
        if start < 0:
            start = max(length + start, 0)
        if stop < 0:
            stop = length + stop
        stop = min(stop, length - 1)
        if start > stop:
            return []
        return [item[1] for item in items[start:stop + 1]]

    def cmd_zrank(self, key: str, member: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_ZSET)
        if err:
            return err
        data = self._db.get(key)
        if not data:
            return None
        sl, scores = data
        if member not in scores:
            return None
        idx = sl.index((scores[member], member))
        return idx

    def cmd_zscore(self, key: str, member: str):
        self._is_expired(key)
        err = self._assert_type(key, TYPE_ZSET)
        if err:
            return err
        data = self._db.get(key)
        if not data:
            return None
        _, scores = data
        score = scores.get(member)
        return str(score) if score is not None else None

    def cmd_zrem(self, key: str, *members):
        if not members:
            return Exception("ERR wrong number of arguments for 'zrem' command")
        self._is_expired(key)
        err = self._assert_type(key, TYPE_ZSET)
        if err:
            return err
        data = self._db.get(key)
        if not data:
            return 0
        sl, scores = data
        removed = 0
        for m in members:
            if m in scores:
                sl.remove((scores[m], m))
                del scores[m]
                removed += 1
        if not scores:
            self._delete_key(key)
        if removed > 0:
            self._log_write(["ZREM", key, *members])
        return removed

    # ─────────────────────────────────────────────
    # Background maintenance
    # ─────────────────────────────────────────────

    def active_expire_cycle(self):
        """Invoked periodically to clean up expired keys in the background."""
        now = time.time()
        expired_keys = [k for k, t in self._expires.items() if now > t]
        for k in expired_keys:
            self._delete_key(k)
        return len(expired_keys)
