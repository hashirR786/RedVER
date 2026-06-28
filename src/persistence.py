import os
import json
from collections import deque
from sortedcontainers import SortedList
from src.protocol import encode_response, parse_resp
from src.storage import StorageEngine, TYPE_STRING, TYPE_LIST, TYPE_HASH, TYPE_SET, TYPE_ZSET


class PersistenceManager:
    """
    Manages durability for RedVER.
    Supports:
      - Append-Only File (AOF) for replaying write transactions.
      - Snapshot (RDB) for bulk saving and restoring database state.

    RDB format (JSON):
      {
        "db": {
          "<key>": {
            "type": "<string|list|hash|set|zset>",
            "value": <type-specific serialized value>
          }
        },
        "expires": { "<key>": <timestamp float> }
      }

    Value encoding per type:
      - string : raw string
      - list   : ["a", "b", "c"]  (left-to-right order)
      - hash   : {"field": "value", ...}
      - set    : ["x", "y", "z"]
      - zset   : [[score, member], ...]  (sorted by score)
    """
    def __init__(self, storage: StorageEngine, aof_path: str = "appendonly.aof", rdb_path: str = "dump.rdb"):
        self.storage = storage
        self.aof_path = aof_path
        self.rdb_path = rdb_path
        self.aof_file = None

        # Hook storage events
        self.storage.set_aof_callback(self.log_write)
        self.storage._save_callback = self.save_snapshot

    def start_aof(self):
        """Starts monitoring and writing to the Append-Only File."""
        self.aof_file = open(self.aof_path, "ab", buffering=0)

    def close(self):
        """Closes any open file descriptors."""
        if self.aof_file:
            try:
                self.aof_file.close()
            except Exception:
                pass
            self.aof_file = None

    def log_write(self, cmd_args: list):
        """Logs modifications to the AOF in RESP format."""
        if not self.aof_file:
            return
        try:
            resp_bytes = encode_response(cmd_args)
            self.aof_file.write(resp_bytes)
            self.aof_file.flush()
        except OSError as e:
            print(f"AOF Write Error: {e}")

    def load_aof(self) -> bool:
        """Reads the AOF file from the start and replays all log entries to storage."""
        if not os.path.exists(self.aof_path):
            return False

        try:
            with open(self.aof_path, "rb") as f:
                data = f.read()

            # Temporarily bypass logging to prevent infinite write loop
            original_callback = self.storage._aof_callback
            self.storage.set_aof_callback(None)

            offset = 0
            try:
                while offset < len(data):
                    cmd_args, consumed = parse_resp(data[offset:])
                    if consumed == 0:
                        break
                    if isinstance(cmd_args, Exception):
                        offset += consumed
                        continue
                    if isinstance(cmd_args, list):
                        self.storage.execute(cmd_args)
                    offset += consumed
            finally:
                self.storage.set_aof_callback(original_callback)
            return True
        except Exception as e:
            print(f"Error loading AOF: {e}")
            return False

    def _serialize_value(self, type_tag: str, value) -> any:
        """Converts an in-memory value into a JSON-serializable structure."""
        if type_tag == TYPE_STRING:
            return value
        elif type_tag == TYPE_LIST:
            return list(value)          # deque -> list (left-to-right)
        elif type_tag == TYPE_HASH:
            return dict(value)          # dict -> dict (already serializable)
        elif type_tag == TYPE_SET:
            return list(value)          # set -> list (order not guaranteed)
        elif type_tag == TYPE_ZSET:
            sl, _ = value
            return [[item[0], item[1]] for item in sl]  # [score, member] pairs
        return str(value)

    def _deserialize_value(self, type_tag: str, raw):
        """Reconstructs the correct Python data structure from serialized JSON."""
        if type_tag == TYPE_STRING:
            return str(raw)
        elif type_tag == TYPE_LIST:
            return deque(raw)
        elif type_tag == TYPE_HASH:
            return dict(raw)
        elif type_tag == TYPE_SET:
            return set(raw)
        elif type_tag == TYPE_ZSET:
            sl = SortedList(key=lambda x: (x[0], x[1]))
            scores = {}
            for pair in raw:
                score, member = float(pair[0]), str(pair[1])
                sl.add((score, member))
                scores[member] = score
            return (sl, scores)
        return str(raw)

    def save_snapshot(self) -> bool:
        """Dumps database state synchronously to a JSON snapshot (RDB)."""
        try:
            db_out = {}
            for key, value in self.storage._db.items():
                type_tag = self.storage._types.get(key, TYPE_STRING)
                db_out[key] = {
                    "type": type_tag,
                    "value": self._serialize_value(type_tag, value)
                }

            state = {
                "db": db_out,
                "expires": self.storage._expires
            }
            temp_path = f"{self.rdb_path}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)

            # Atomic replacement
            if os.path.exists(self.rdb_path):
                os.remove(self.rdb_path)
            os.rename(temp_path, self.rdb_path)
            return True
        except Exception as e:
            print(f"Error writing snapshot: {e}")
            return False

    def load_snapshot(self) -> bool:
        """Loads database state from the snapshot file (RDB)."""
        if not os.path.exists(self.rdb_path):
            return False
        try:
            with open(self.rdb_path, "r", encoding="utf-8") as f:
                state = json.load(f)

            db_raw = state.get("db", {})
            new_db = {}
            new_types = {}

            for key, entry in db_raw.items():
                # Support both new tagged format and legacy plain-string format
                if isinstance(entry, dict) and "type" in entry and "value" in entry:
                    type_tag = entry["type"]
                    new_db[key] = self._deserialize_value(type_tag, entry["value"])
                    new_types[key] = type_tag
                else:
                    # Legacy format: plain string value
                    new_db[key] = str(entry)
                    new_types[key] = TYPE_STRING

            self.storage._db = new_db
            self.storage._types = new_types
            self.storage._expires = {
                str(k): float(v) for k, v in state.get("expires", {}).items()
            }
            return True
        except Exception as e:
            print(f"Error reading snapshot: {e}")
            return False
