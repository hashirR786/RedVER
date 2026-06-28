"""
RedVER Protocol Module — RESP2 + RESP3 parser and encoder.

Supports:
  RESP2 types:  Simple String (+), Error (-), Integer (:), Bulk String ($), Array (*)
  RESP3 types:  Map (%), Double (,), Boolean (#), Blob Error (!), Verbatim String (=),
                Big Number ((), Null (_), Set (~)  [subset used in HELLO 3 handshake]
  Inline:       Raw text commands from telnet/netcat, with shlex-style quoted token support
"""

import shlex


class SimpleString:
    """Represents a RESP Simple String (e.g. +OK\\r\\n)."""
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f"SimpleString({self.value!r})"

    def __eq__(self, other):
        return isinstance(other, SimpleString) and self.value == other.value


class RespMap:
    """
    Represents a RESP3 Map reply (%<count>\\r\\n <key><value> ...).
    Wraps a plain Python dict for encoding purposes.
    """
    def __init__(self, data: dict):
        self.data = data

    def __repr__(self):
        return f"RespMap({self.data!r})"


class RespDouble:
    """Represents a RESP3 Double reply (,<value>\\r\\n)."""
    def __init__(self, value: float):
        self.value = value

    def __repr__(self):
        return f"RespDouble({self.value!r})"


# ──────────────────────────────────────────────────────────────────────────────
# Parser
# ──────────────────────────────────────────────────────────────────────────────

def parse_resp(data: bytes) -> tuple:
    """
    Parses a single RESP2/RESP3 object from the start of data.
    Returns (parsed_value, bytes_consumed).

    Returns (None, 0) when data is incomplete (caller should buffer and retry).
    Returns (Exception, N) on a protocol violation (N bytes to discard).
    """
    if not data:
        return None, 0

    prefix = data[0:1]

    # ── RESP2 Simple String ────────────────────────────────────────────────
    if prefix == b'+':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        val = data[1:idx].decode('utf-8', errors='replace')
        return SimpleString(val), idx + 2

    # ── RESP2 Error ───────────────────────────────────────────────────────
    elif prefix == b'-':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        val = data[1:idx].decode('utf-8', errors='replace')
        return Exception(val), idx + 2

    # ── RESP2 Integer ─────────────────────────────────────────────────────
    elif prefix == b':':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        try:
            val = int(data[1:idx].decode('utf-8', errors='replace'))
            return val, idx + 2
        except ValueError:
            return Exception("ERR Protocol error: invalid integer value"), idx + 2

    # ── RESP2 Bulk String ─────────────────────────────────────────────────
    elif prefix == b'$':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        try:
            length = int(data[1:idx].decode('utf-8', errors='replace'))
        except ValueError:
            return Exception("ERR Protocol error: invalid bulk string length"), idx + 2

        if length == -1:          # RESP2 null bulk string
            return None, idx + 2

        end = idx + 2 + length
        if len(data) < end + 2:   # not enough data yet
            return None, 0

        if data[end:end + 2] != b'\r\n':
            return Exception("ERR Protocol error: invalid bulk string terminator"), end + 2

        val = data[idx + 2:end].decode('utf-8', errors='replace')
        return val, end + 2

    # ── RESP2/3 Array (*) and RESP3 Set (~) — both parse the same way ─────
    elif prefix in (b'*', b'~'):
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        try:
            count = int(data[1:idx].decode('utf-8', errors='replace'))
        except ValueError:
            return Exception("ERR Protocol error: invalid array count"), idx + 2

        if count == -1:
            return None, idx + 2   # RESP2 null array
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

    # ── RESP3 Map (%) ─────────────────────────────────────────────────────
    elif prefix == b'%':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        try:
            pair_count = int(data[1:idx].decode('utf-8', errors='replace'))
        except ValueError:
            return Exception("ERR Protocol error: invalid map count"), idx + 2

        offset = idx + 2
        result = {}
        for _ in range(pair_count):
            # Parse key
            k, consumed = parse_resp(data[offset:])
            if consumed == 0:
                return None, 0
            offset += consumed
            # Parse value
            v, consumed = parse_resp(data[offset:])
            if consumed == 0:
                return None, 0
            offset += consumed
            result[k] = v
        return RespMap(result), offset

    # ── RESP3 Double (,) ──────────────────────────────────────────────────
    elif prefix == b',':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        raw = data[1:idx].decode('utf-8', errors='replace')
        if raw in ('inf', '+inf'):
            return RespDouble(float('inf')), idx + 2
        if raw == '-inf':
            return RespDouble(float('-inf')), idx + 2
        try:
            return RespDouble(float(raw)), idx + 2
        except ValueError:
            return Exception("ERR Protocol error: invalid double value"), idx + 2

    # ── RESP3 Boolean (#) ─────────────────────────────────────────────────
    elif prefix == b'#':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        raw = data[1:idx].decode('utf-8', errors='replace')
        if raw == 't':
            return True, idx + 2
        elif raw == 'f':
            return False, idx + 2
        return Exception("ERR Protocol error: invalid boolean value"), idx + 2

    # ── RESP3 Null (_) ────────────────────────────────────────────────────
    elif prefix == b'_':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        return None, idx + 2

    # ── RESP3 Big Number (() ──────────────────────────────────────────────
    elif prefix == b'(':
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        try:
            val = int(data[1:idx].decode('utf-8', errors='replace'))
            return val, idx + 2
        except ValueError:
            return Exception("ERR Protocol error: invalid big number"), idx + 2

    # ── RESP3 Blob Error (!) ──────────────────────────────────────────────
    elif prefix == b'!':
        # Same wire format as Bulk String but signals an error
        idx = data.find(b'\r\n')
        if idx == -1:
            return None, 0
        try:
            length = int(data[1:idx].decode('utf-8', errors='replace'))
        except ValueError:
            return Exception("ERR Protocol error: invalid blob error length"), idx + 2
        end = idx + 2 + length
        if len(data) < end + 2:
            return None, 0
        val = data[idx + 2:end].decode('utf-8', errors='replace')
        return Exception(val), end + 2

    # ── Inline command fallback (telnet / netcat) ─────────────────────────
    else:
        # Find line terminator (\r\n preferred, \n accepted)
        crlf = data.find(b'\r\n')
        lf   = data.find(b'\n')

        if crlf == -1 and lf == -1:
            return None, 0                    # incomplete, wait for more data

        if crlf != -1 and (lf == -1 or crlf <= lf):
            line_bytes = data[:crlf]
            consumed   = crlf + 2
        else:
            line_bytes = data[:lf]
            consumed   = lf + 1

        line = line_bytes.decode('utf-8', errors='replace').strip()
        if not line:
            return [], consumed               # blank line → empty command (skip)

        # Use shlex to honour quoted tokens: SET key "hello world"
        try:
            tokens = shlex.split(line)
        except ValueError:
            # Malformed quoting → fall back to simple whitespace split
            tokens = line.split()

        return tokens, consumed


# ──────────────────────────────────────────────────────────────────────────────
# Encoder  (RESP2 default;  RESP3 types used when encoding RespMap / RespDouble)
# ──────────────────────────────────────────────────────────────────────────────

def encode_response(data: any, resp3: bool = False) -> bytes:
    """
    Encodes a Python object into RESP2 (default) or RESP3 byte representation.

    RESP3 is activated per-connection when the client sends HELLO 3.
    Only the relevant RESP3 wire types are used:
      - RespMap  → %<n>\\r\\n (key value) ...
      - RespDouble → ,<value>\\r\\n
      - bool     → #t\\r\\n / #f\\r\\n   (RESP3 only; RESP2 uses :1/:0)
      - None     → _\\r\\n             (RESP3 only; RESP2 uses $-1\\r\\n)
      - dict     → %<n>\\r\\n ...       (RESP3 only; RESP2 flattens to array)
    """
    # ── Null ──────────────────────────────────────────────────────────────
    if data is None:
        return b"_\r\n" if resp3 else b"$-1\r\n"

    # ── Simple String ─────────────────────────────────────────────────────
    if isinstance(data, SimpleString):
        return f"+{data.value}\r\n".encode('utf-8')

    # ── Error ─────────────────────────────────────────────────────────────
    if isinstance(data, Exception):
        err_msg = str(data)
        if not err_msg.startswith("ERR ") and not err_msg.startswith("WRONGTYPE "):
            err_msg = f"ERR {err_msg}"
        return f"-{err_msg}\r\n".encode('utf-8')

    # ── Boolean ───────────────────────────────────────────────────────────
    if isinstance(data, bool):
        if resp3:
            return b"#t\r\n" if data else b"#f\r\n"
        return b":1\r\n" if data else b":0\r\n"

    # ── Integer ───────────────────────────────────────────────────────────
    if isinstance(data, int):
        return f":{data}\r\n".encode('utf-8')

    # ── RESP3 Double ──────────────────────────────────────────────────────
    if isinstance(data, RespDouble):
        v = data.value
        if v == float('inf'):
            raw = 'inf'
        elif v == float('-inf'):
            raw = '-inf'
        else:
            raw = repr(v)
        return f",{raw}\r\n".encode('utf-8')

    # ── Float (encode as double in RESP3, bulk string in RESP2) ──────────
    if isinstance(data, float):
        if resp3:
            return encode_response(RespDouble(data), resp3=True)
        encoded = str(data).encode('utf-8')
        return f"${len(encoded)}\r\n".encode('utf-8') + encoded + b"\r\n"

    # ── String ────────────────────────────────────────────────────────────
    if isinstance(data, str):
        encoded = data.encode('utf-8')
        return f"${len(encoded)}\r\n".encode('utf-8') + encoded + b"\r\n"

    # ── Bytes ─────────────────────────────────────────────────────────────
    if isinstance(data, bytes):
        return f"${len(data)}\r\n".encode('utf-8') + data + b"\r\n"

    # ── RESP3 Map ─────────────────────────────────────────────────────────
    if isinstance(data, RespMap):
        res = f"%{len(data.data)}\r\n".encode('utf-8')
        for k, v in data.data.items():
            res += encode_response(k, resp3)
            res += encode_response(v, resp3)
        return res

    # ── Plain dict — emit as RESP3 map or RESP2 flat array ────────────────
    if isinstance(data, dict):
        if resp3:
            return encode_response(RespMap(data), resp3=True)
        # RESP2: flatten to [k, v, k, v, ...]
        flat = []
        for k, v in data.items():
            flat.extend([k, v])
        res = f"*{len(flat)}\r\n".encode('utf-8')
        for item in flat:
            res += encode_response(item, resp3)
        return res

    # ── List / Tuple ──────────────────────────────────────────────────────
    if isinstance(data, (list, tuple)):
        res = f"*{len(data)}\r\n".encode('utf-8')
        for item in data:
            res += encode_response(item, resp3)
        return res

    # ── Fallback: stringify ───────────────────────────────────────────────
    encoded = str(data).encode('utf-8')
    return f"${len(encoded)}\r\n".encode('utf-8') + encoded + b"\r\n"
