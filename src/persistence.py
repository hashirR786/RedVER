import os
import json
from src.protocol import encode_response, parse_resp
from src.storage import StorageEngine

class PersistenceManager:
    """
    Manages durability for RedVER.
    Supports:
      - Append-Only File (AOF) for replaying write transactions.
      - Snapshot (RDB) for bulk saving database state.
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
        # Open in append mode, binary, unbuffered (buffering=0 requires binary mode)
        self.aof_file = open(self.aof_path, "ab", buffering=0)

    def close(self):
        """Closes any open file descriptors."""
        if self.aof_file:
            try:
                self.aof_file.close()
            except Exception:
                pass
            self.aof_file = None

    def log_write(self, cmd_args: list[str]):
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
            
            # Temporarily bypass logging to AOF while recovering to prevent infinite write loop
            original_callback = self.storage._aof_callback
            self.storage.set_aof_callback(None)
            
            offset = 0
            try:
                while offset < len(data):
                    cmd_args, consumed = parse_resp(data[offset:])
                    if consumed == 0:
                        break
                    
                    if isinstance(cmd_args, Exception):
                        # If corruption found, skip the rest of this command segment
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

    def save_snapshot(self) -> bool:
        """Dumps database state synchronously to a JSON snapshot."""
        try:
            # We dump the memory structure
            state = {
                "db": self.storage._db,
                "expires": self.storage._expires
            }
            temp_path = f"{self.rdb_path}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
                
            # Perform atomic replacement
            if os.path.exists(self.rdb_path):
                os.remove(self.rdb_path)
            os.rename(temp_path, self.rdb_path)
            return True
        except Exception as e:
            print(f"Error writing snapshot: {e}")
            return False

    def load_snapshot(self) -> bool:
        """Loads database state from the snapshot file."""
        if not os.path.exists(self.rdb_path):
            return False
        try:
            with open(self.rdb_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            
            self.storage._db = state.get("db", {})
            raw_expires = state.get("expires", {})
            
            # Rehydrate keys and convert floats
            self.storage._expires = {str(k): float(v) for k, v in raw_expires.items()}
            return True
        except Exception as e:
            print(f"Error reading snapshot: {e}")
            return False
