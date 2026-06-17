import socket
import sys
import shlex
import os
from src.protocol import parse_resp, encode_response, SimpleString

# ANSI escape sequences for premium colors
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BLUE = "\033[94m"
GRAY = "\033[90m"

# Enable ANSI escape processing on Windows consoles
if sys.platform == "win32":
    os.system("")

def print_welcome_banner():
    banner = f"""
{CYAN}{BOLD}██████╗ ███████╗██████╗ ██╗   ██╗███████╗██████╗ 
██╔══██╗██╔════╝██╔══██╗██║   ██║██╔════╝██╔══██╗
██████╔╝█████╗  ██║  ██║██║   ██║█████╗  ██████╔╝
██╔══██╗██╔══╝  ██║  ██║╚██╗ ██╔╝██╔══╝  ██╔══██╗
██║  ██║███████╗██████╔╝ ╚████╔╝ ███████╗██║  ██║
╚═╝  ╚═╝╚══════╝╚═════╝   ╚═══╝  ╚══════╝╚═╝  ╚═╝{RESET}
             {GREEN}Redis-Compatible Cache Engine{RESET}
             {GRAY}Type 'QUIT' to exit.{RESET}
    """
    print(banner)

def format_value_inline(val) -> str:
    """Formats values for single-line display inside arrays."""
    if val is None:
        return f"{YELLOW}(nil){RESET}"
    elif isinstance(val, SimpleString):
        return f"{GREEN}{val.value}{RESET}"
    elif isinstance(val, Exception):
        return f"{RED}(error) {val}{RESET}"
    elif isinstance(val, int) and not isinstance(val, bool):
        return f"{BLUE}(integer) {val}{RESET}"
    elif isinstance(val, bool):
        return f"{BLUE}(integer) {1 if val else 0}{RESET}"
    elif isinstance(val, str):
        return f"\"{val}\""
    else:
        return f"\"{val}\""

def print_formatted(val, indent: int = 0):
    """Outputs server response in a readable Redis CLI format."""
    ind = "  " * indent
    if val is None:
        print(f"{ind}{YELLOW}(nil){RESET}")
    elif isinstance(val, SimpleString):
        print(f"{ind}{GREEN}{val.value}{RESET}")
    elif isinstance(val, Exception):
        print(f"{ind}{RED}(error) {val}{RESET}")
    elif isinstance(val, int) and not isinstance(val, bool):
        print(f"{ind}{BLUE}(integer) {val}{RESET}")
    elif isinstance(val, bool):
        print(f"{ind}{BLUE}(integer) {1 if val else 0}{RESET}")
    elif isinstance(val, str):
        # Print string response. If it contains newlines, print raw, otherwise with quotes.
        if "\n" in val:
            print(f"{val}")
        else:
            print(f"{ind}\"{val}\"")
    elif isinstance(val, list):
        if not val:
            print(f"{ind}(empty array)")
            return
        for i, item in enumerate(val, start=1):
            if isinstance(item, list):
                print(f"{ind}{i})")
                print_formatted(item, indent + 1)
            else:
                print(f"{ind}{i}) {format_value_inline(item)}")
    else:
        print(f"{ind}\"{val}\"")

def read_response(sock: socket.socket) -> any:
    """Reads stream from socket and parses RESP response."""
    buf = bytearray()
    while True:
        data = sock.recv(4096)
        if not data:
            raise ConnectionError("Server connection closed.")
        buf.extend(data)
        val, consumed = parse_resp(bytes(buf))
        if consumed > 0:
            return val

def run_client(host: str, port: int):
    """Starts the client interactive socket session."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        print_welcome_banner()
        print(f"[+] Connected to RedVER Server at {host}:{port}\n")
    except Exception as e:
        print(f"{RED}Error: Could not connect to RedVER Server at {host}:{port}: {e}{RESET}")
        return

    try:
        while True:
            # Stylized prompt
            prompt = f"{CYAN}{host}:{port}>{RESET} "
            try:
                line = input(prompt)
            except EOFError:
                break
                
            line = line.strip()
            if not line:
                continue

            try:
                # Handle command line tokenization preserving quotes
                args = shlex.split(line)
            except ValueError as e:
                print(f"{RED}(error) Syntax error: {e}{RESET}")
                continue

            if not args:
                continue

            # Command QUIT logic locally or sent to server
            cmd_upper = args[0].upper()
            
            # Send serialized command
            try:
                payload = encode_response(args)
                sock.sendall(payload)
            except Exception as e:
                print(f"{RED}Error sending command: {e}{RESET}")
                break

            # Read response
            try:
                res = read_response(sock)
                print_formatted(res)
            except ConnectionError as e:
                print(f"{RED}{e}{RESET}")
                break
            except Exception as e:
                print(f"{RED}Error reading reply: {e}{RESET}")
                break

            if cmd_upper == "QUIT":
                break
                
    except KeyboardInterrupt:
        print("\n[*] Exiting client.")
    finally:
        sock.close()
        print("[*] Connection closed.")


if __name__ == "__main__":
    host = "127.0.0.1"
    port = 6379
    if len(sys.argv) > 1:
        parts = sys.argv[1].split(":")
        if len(parts) == 2:
            host = parts[0]
            try:
                port = int(parts[1])
            except ValueError:
                pass
        else:
            host = sys.argv[1]
            if len(sys.argv) > 2:
                try:
                    port = int(sys.argv[2])
                except ValueError:
                    pass
                    
    run_client(host, port)
