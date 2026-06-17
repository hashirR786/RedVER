import time
import fnmatch
from src.protocol import SimpleString

class StorageEngine:
    """
    In-memory database engine for RedVER.
    Handles basic commands, TTL/expiration, and hooks for logging write operations.
    """
    def __init__(self):
        self._db = {}       # key (str) -> value (str)
        self._expires = {}  # key (str) -> expiration timestamp (float)
        self._aof_callback = None

    def set_aof_callback(self, callback):
        """Sets the write callback function that intercepts modifying commands."""
        self._aof_callback = callback

    def _log_write(self, cmd_args: list[str]):
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
            # Evict key
            del self._db[key]
            del self._expires[key]
            return True
        return False

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
            # Let's inspect parameter signature or just provide standard error
            return Exception(f"ERR wrong number of arguments for '{cmd.lower()}' command")
        except Exception as e:
            return Exception(f"ERR {str(e)}")

    # Core commands implementation
    
    def cmd_ping(self, *args):
        if not args:
            return SimpleString("PONG")
        return args[0]

    def cmd_set(self, key: str, value: str, *options):
        # Syntax: SET key value [EX seconds]
        expiry_sec = None
        if options:
            if len(options) == 2 and str(options[0]).upper() == "EX":
                try:
                    expiry_sec = int(options[1])
                except ValueError:
                    return Exception("ERR value is not an integer or out of range")
            else:
                return Exception("ERR syntax error")

        self._db[key] = str(value)
        if expiry_sec is not None:
            self._expires[key] = time.time() + expiry_sec
            self._log_write(["SET", key, str(value), "EX", str(expiry_sec)])
        else:
            # Remove any existing expiry on override
            if key in self._expires:
                del self._expires[key]
            self._log_write(["SET", key, str(value)])
            
        return SimpleString("OK")

    def cmd_get(self, key: str):
        if self._is_expired(key):
            return None
        return self._db.get(key, None)

    def cmd_hello(self, *args):
        # Return a standard RESP2 server info handshake array to satisfy modern clients (like node-redis)
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
        self._is_expired(key)  # passive evict if needed
        val_str = self._db.get(key, "0")
        try:
            val_int = int(val_str)
        except ValueError:
            return Exception("ERR value is not an integer or out of range")
        
        new_val = val_int + 1
        self._db[key] = str(new_val)
        self._log_write(["SET", key, str(new_val)])
        return new_val

    def cmd_del(self, *keys):
        if not keys:
            return Exception("ERR wrong number of arguments for 'del' command")
        deleted = 0
        log_keys = []
        for key in keys:
            self._is_expired(key)  # passive evict if needed
            if key in self._db:
                del self._db[key]
                if key in self._expires:
                    del self._expires[key]
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
        # Refresh state by passive evicting expired keys
        all_keys = list(self._db.keys())
        for key in all_keys:
            self._is_expired(key)
            
        matched = []
        for key in self._db:
            if fnmatch.fnmatch(key, pattern):
                matched.append(key)
        return matched

    def cmd_flushdb(self):
        self._db.clear()
        self._expires.clear()
        self._log_write(["FLUSHDB"])
        return SimpleString("OK")
        
    def active_expire_cycle(self):
        """Invoked periodically to clean up expired keys in the background."""
        now = time.time()
        expired_keys = [k for k, expire_time in self._expires.items() if now > expire_time]
        for k in expired_keys:
            if k in self._db:
                del self._db[k]
            if k in self._expires:
                del self._expires[k]
        return len(expired_keys)
