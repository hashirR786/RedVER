import asyncio
import argparse
import sys
import os
import time
import json
import shlex
from src.protocol import parse_resp, encode_response, SimpleString
from src.storage import StorageEngine
from src.persistence import PersistenceManager

class RedisServer:
    """
    TCP server for RedVER, compatible with RESP.
    Also hosts a self-contained HTTP web dashboard.
    Uses asyncio for high concurrency, non-blocking I/O.
    """
    def __init__(self, host: str = "127.0.0.1", port: int = 6379, http_port: int = 8080, aof_enabled: bool = True):
        self.host = host
        self.port = port
        self.http_port = http_port
        self.aof_enabled = aof_enabled
        
        self.storage = StorageEngine()
        self.persistence = PersistenceManager(self.storage)
        self.server = None
        self.http_server = None
        self._active_expire_task = None
        self.is_running = False
        
        # Dashboard metrics
        self.start_time = time.time()
        self.stats_ops_count = 0

    async def start(self):
        """Starts the server, loads state from disk, and registers tasks."""
        # 1. Recover database state from file system
        print("[*] Loading database state...")
        recovered = False
        if self.aof_enabled:
            recovered = self.persistence.load_aof()
            if recovered:
                print("[+] State recovered successfully from Append-Only File (AOF).")
        
        if not recovered:
            if self.persistence.load_snapshot():
                print("[+] State recovered successfully from Snapshot (RDB).")
            else:
                print("[*] No existing data store found. Starting with clean keyspace.")

        # 2. Open AOF for active append operations
        if self.aof_enabled:
            self.persistence.start_aof()
            print("[*] Append-Only File durability active.")

        # 3. Start asyncio TCP Server & HTTP server
        self.is_running = True
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        self.http_server = await asyncio.start_server(
            self.handle_http_client, self.host, self.http_port
        )
        print(f"[+] RedVER Server listening on {self.host}:{self.port} (TCP/RESP)")
        print(f"[+] Web Dashboard available at http://{self.host}:{self.http_port} (HTTP)")
        
        # 4. Schedule active key eviction task
        self._active_expire_task = asyncio.create_task(self._active_expire_loop())

        try:
            # Run both servers concurrently
            await asyncio.gather(
                self.server.serve_forever(),
                self.http_server.serve_forever()
            )
        except asyncio.CancelledError:
            pass
        finally:
            self.stop()

    async def _active_expire_loop(self):
        """Periodically cleans up expired keys in the background."""
        while self.is_running:
            try:
                await asyncio.sleep(1.0)
                evicted = self.storage.active_expire_cycle()
                if evicted > 0:
                    print(f"[*] Eviction Cycle: Cleaned up {evicted} expired keys from memory.")
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[-] Error in eviction cycle: {e}")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handles incoming client connection and maps requests onto StorageEngine."""
        addr = writer.get_extra_info('peername')
        print(f"[+] Client connected from {addr[0]}:{addr[1]}")
        
        buf = bytearray()
        try:
            while self.is_running:
                data = await reader.read(4096)
                if not data:
                    break
                
                buf.extend(data)
                
                while True:
                    cmd_args, consumed = parse_resp(bytes(buf))
                    if consumed == 0:
                        # Incomplete packet, wait for more data
                        break
                    
                    del buf[:consumed]
                    
                    if cmd_args is None:
                        continue
                        
                    if isinstance(cmd_args, Exception):
                        writer.write(encode_response(cmd_args))
                        await writer.drain()
                        continue
                    
                    if not isinstance(cmd_args, list):
                        # Wrap raw/inline commands if needed
                        cmd_args = [cmd_args]
                        
                    if not cmd_args:
                        continue
                        
                    cmd_name = str(cmd_args[0]).upper()
                    
                    # Intercept QUIT command
                    if cmd_name == "QUIT":
                        writer.write(encode_response(SimpleString("OK")))
                        await writer.drain()
                        break
                        
                    # Standard execution
                    result = self.storage.execute(cmd_args)
                    self.stats_ops_count += 1
                    writer.write(encode_response(result))
                    await writer.drain()
                    
                if not data or (cmd_args and str(cmd_args[0]).upper() == "QUIT"):
                    break
                    
        except asyncio.IncompleteReadError:
            pass
        except ConnectionResetError:
            pass
        except Exception as e:
            print(f"[-] Error processing client {addr[0]}:{addr[1]}: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            print(f"[-] Client disconnected from {addr[0]}:{addr[1]}")

    async def handle_http_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handles incoming HTTP requests for the Web Dashboard and API endpoints."""
        try:
            header_data = bytearray()
            while True:
                chunk = await reader.read(1024)
                if not chunk:
                    break
                header_data.extend(chunk)
                if b"\r\n\r\n" in header_data:
                    break
                    
            if not header_data:
                return
                
            parts = header_data.split(b"\r\n\r\n", 1)
            header_bytes = parts[0]
            body_bytes = parts[1] if len(parts) > 1 else b""
            
            header_lines = header_bytes.decode('utf-8', errors='replace').split("\r\n")
            request_line = header_lines[0]
            req_parts = request_line.split()
            if len(req_parts) < 2:
                return
                
            method = req_parts[0].upper()
            path = req_parts[1]
            
            content_length = 0
            for line in header_lines[1:]:
                if line.lower().startswith("content-length:"):
                    try:
                        content_length = int(line.split(":", 1)[1].strip())
                    except ValueError:
                        pass
                    break
                    
            if len(body_bytes) < content_length:
                needed = content_length - len(body_bytes)
                body_bytes += await reader.readexactly(needed)
                
            cors_headers = "Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type\r\n"
            
            if method == "OPTIONS":
                response = f"HTTP/1.1 200 OK\r\n{cors_headers}\r\n".encode('utf-8')
                writer.write(response)
                await writer.drain()
                return

            if method == "GET":
                if path == "/" or path == "/index.html":
                    try:
                        dir_path = os.path.dirname(os.path.abspath(__file__))
                        html_path = os.path.join(dir_path, "dashboard.html")
                        with open(html_path, "r", encoding="utf-8") as f:
                            html_content = f.read()
                            
                        resp_bytes = html_content.encode('utf-8')
                        response = (
                            f"HTTP/1.1 200 OK\r\n"
                            f"Content-Type: text/html; charset=utf-8\r\n"
                            f"Content-Length: {len(resp_bytes)}\r\n"
                            f"\r\n"
                        ).encode('utf-8') + resp_bytes
                    except Exception as e:
                        err_bytes = f"Error reading dashboard: {e}".encode('utf-8')
                        response = (
                            f"HTTP/1.1 500 Internal Server Error\r\n"
                            f"Content-Type: text/plain\r\n"
                            f"Content-Length: {len(err_bytes)}\r\n"
                            f"\r\n"
                        ).encode('utf-8') + err_bytes
                elif path == "/api/keys":
                    # Passive evict expired keys
                    all_keys = list(self.storage._db.keys())
                    for k in all_keys:
                        self.storage._is_expired(k)
                        
                    keys_data = []
                    for k in self.storage._db:
                        ttl = self.storage.cmd_ttl(k)
                        keys_data.append({
                            "name": k,
                            "value": self.storage._db[k],
                            "ttl": ttl
                        })
                        
                    json_str = json.dumps({"keys": keys_data})
                    resp_bytes = json_str.encode('utf-8')
                    response = (
                        f"HTTP/1.1 200 OK\r\n"
                        f"Content-Type: application/json\r\n"
                        f"Content-Length: {len(resp_bytes)}\r\n"
                        f"{cors_headers}"
                        f"\r\n"
                    ).encode('utf-8') + resp_bytes
                elif path == "/api/stats":
                    stats = {
                        "uptime": int(time.time() - self.start_time),
                        "ops": self.stats_ops_count,
                        "keys_count": len(self.storage._db),
                        "aof_enabled": self.aof_enabled,
                        "aof_size": os.path.getsize(self.persistence.aof_path) if (self.aof_enabled and os.path.exists(self.persistence.aof_path)) else 0,
                        "rdb_size": os.path.getsize(self.persistence.rdb_path) if os.path.exists(self.persistence.rdb_path) else 0
                    }
                    resp_bytes = json.dumps(stats).encode('utf-8')
                    response = (
                        f"HTTP/1.1 200 OK\r\n"
                        f"Content-Type: application/json\r\n"
                        f"Content-Length: {len(resp_bytes)}\r\n"
                        f"{cors_headers}"
                        f"\r\n"
                    ).encode('utf-8') + resp_bytes
                else:
                    response = b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found"
            elif method == "POST" and path == "/api/exec":
                try:
                    body_str = body_bytes.decode('utf-8')
                    req_data = json.loads(body_str) if body_str else {}
                    cmd_input = req_data.get("cmd", "")
                    
                    if isinstance(cmd_input, list):
                        cmd_args = cmd_input
                    else:
                        cmd_args = shlex.split(str(cmd_input))
                        
                    if not cmd_args:
                        raise ValueError("Empty command")
                        
                    result = self.storage.execute(cmd_args)
                    self.stats_ops_count += 1
                    
                    if isinstance(result, SimpleString):
                        json_resp = {"status": "success", "result": result.value, "type": "status"}
                    elif isinstance(result, Exception):
                        json_resp = {"status": "error", "error": str(result)}
                    elif result is None:
                        json_resp = {"status": "success", "result": None, "type": "nil"}
                    elif isinstance(result, list):
                        formatted_list = [x.value if isinstance(x, SimpleString) else str(x) if isinstance(x, Exception) else x for x in result]
                        json_resp = {"status": "success", "result": formatted_list, "type": "array"}
                    else:
                        json_resp = {"status": "success", "result": result, "type": "string" if isinstance(result, str) else "integer"}
                except Exception as e:
                    json_resp = {"status": "error", "error": f"Execution error: {e}"}
                    
                resp_bytes = json.dumps(json_resp).encode('utf-8')
                response = (
                    f"HTTP/1.1 200 OK\r\n"
                    f"Content-Type: application/json\r\n"
                    f"Content-Length: {len(resp_bytes)}\r\n"
                    f"{cors_headers}"
                    f"\r\n"
                ).encode('utf-8') + resp_bytes
            else:
                response = b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 18\r\n\r\nMethod Not Allowed"
                
            writer.write(response)
            await writer.drain()
        except Exception as e:
            print(f"[-] Error in HTTP service: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    def stop(self):
        """Teardown server tasks and close handles."""
        self.is_running = False
        if self._active_expire_task:
            self._active_expire_task.cancel()
        if self.server:
            self.server.close()
        if self.http_server:
            self.http_server.close()
        self.persistence.close()
        print("[*] Server shutdown clean.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RedVER: A lightweight Redis-compatible cache.")
    parser.add_argument("--host", default="127.0.0.1", help="Host address to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=6379, help="Port to listen on (default: 6379)")
    parser.add_argument("--http-port", type=int, default=8080, help="Port for HTTP Dashboard UI (default: 8080)")
    parser.add_argument("--no-aof", action="store_true", help="Disable Append-Only File persistence")
    
    args = parser.parse_args()
    
    server = RedisServer(host=args.host, port=args.port, http_port=args.http_port, aof_enabled=not args.no_aof)
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("\n[*] Interrupted by user. Exiting.")
        sys.exit(0)

