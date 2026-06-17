class SimpleString:
    """Represents a RESP Simple String (e.g. +OK\\r\\n)."""
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f"SimpleString({self.value!r})"

    def __eq__(self, other):
        return isinstance(other, SimpleString) and self.value == other.value


def parse_resp(data: bytes) -> tuple[any, int]:
    """
    Parses a single RESP object from the start of data.
    Returns (parsed_value, bytes_consumed).
    If a complete RESP message cannot be parsed yet, returns (None, 0).
    If a protocol violation is detected, returns (Exception, bytes_consumed).
    
    Supports basic raw command line fallback (whitespace separated) if data 
    does not start with a RESP prefix character (+, -, :, $, *).
    """
    if not data:
        return None, 0

    prefix = data[0:1]
    
    if prefix == b'+':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        val = data[1:idx].decode('utf-8', errors='replace')
        return SimpleString(val), idx + 2

    elif prefix == b'-':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        val = data[1:idx].decode('utf-8', errors='replace')
        return Exception(val), idx + 2

    elif prefix == b':':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        try:
            val = int(data[1:idx].decode('utf-8', errors='replace'))
            return val, idx + 2
        except ValueError:
            return Exception("ERR Protocol error: invalid integer value"), idx + 2

    elif prefix == b'$':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        try:
            length = int(data[1:idx].decode('utf-8', errors='replace'))
        except ValueError:
            return Exception("ERR Protocol error: invalid bulk string length"), idx + 2

        if length == -1:
            return None, idx + 2

        if len(data) < idx + 2 + length + 2:
            return None, 0

        # Ensure correct \r\n terminator for bulk string
        if data[idx + 2 + length : idx + 2 + length + 2] != b'\r\n':
            return Exception("ERR Protocol error: invalid bulk string terminator"), idx + 2 + length + 2

        val = data[idx + 2 : idx + 2 + length].decode('utf-8', errors='replace')
        return val, idx + 2 + length + 2

    elif prefix == b'*':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        try:
            count = int(data[1:idx].decode('utf-8', errors='replace'))
        except ValueError:
            return Exception("ERR Protocol error: invalid array elements count"), idx + 2

        if count == -1:
            return None, idx + 2

        if count == 0:
            return [], idx + 2

        offset = idx + 2
        items = []
        for _ in range(count):
            if offset >= len(data):
                return None, 0
            item, consumed = parse_resp(data[offset:])
            if consumed == 0:
                return None, 0
            if isinstance(item, Exception):
                return item, offset + consumed
            items.append(item)
            offset += consumed
        return items, offset

    else:
        # Inline commands fallback (e.g. from netcat or telnet: "PING\r\n" or "SET a b\n")
        idx = data.find(b'\r\n')
        if idx == -1:
            # Check if there is at least a \n
            idx = data.find(b'\n')
            if idx == -1:
                return None, 0
            line = data[:idx].decode('utf-8', errors='replace').strip()
            consumed = idx + 1
        else:
            line = data[:idx].decode('utf-8', errors='replace').strip()
            consumed = idx + 2
            
        if not line:
            return [], consumed
            
        return line.split(), consumed


def encode_response(data: any) -> bytes:
    """Encodes a Python object into RESP byte representation."""
    if data is None:
        return b"$-1\r\n"
        
    if isinstance(data, SimpleString):
        return f"+{data.value}\r\n".encode('utf-8')
        
    if isinstance(data, Exception):
        err_msg = str(data)
        if not err_msg.startswith("ERR "):
            err_msg = f"ERR {err_msg}"
        return f"-{err_msg}\r\n".encode('utf-8')
        
    if isinstance(data, bool):
        # Redis uses 1 and 0 integers for boolean replies
        return b":1\r\n" if data else b":0\r\n"
        
    if isinstance(data, int):
        return f":{data}\r\n".encode('utf-8')
        
    if isinstance(data, str):
        # Default string encoding as bulk string
        encoded = data.encode('utf-8')
        return f"${len(encoded)}\r\n".encode('utf-8') + encoded + b"\r\n"
        
    if isinstance(data, bytes):
        return f"${len(data)}\r\n".encode('utf-8') + data + b"\r\n"
        
    if isinstance(data, (list, tuple)):
        # Array response
        res = f"*{len(data)}\r\n".encode('utf-8')
        for item in data:
            res += encode_response(item)
        return res
        
    # Fallback to bulk string of string representation
    encoded = str(data).encode('utf-8')
    return f"${len(encoded)}\r\n".encode('utf-8') + encoded + b"\r\n"
