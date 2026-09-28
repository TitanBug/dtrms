"""
TeleRM wire protocol (M3: migration messages; M4: HMAC authentication).

Every message is a JSON object preceded by a 4-byte big-endian length
prefix (design doc section 6: "4-byte length plus JSON over TCP. Each
exchange is one request and one reply.").

Milestone 3 additions
----------------------
Three new request/reply exchanges for live instance migration:

    INSTANCE_MIGRATE   (manager  -> source site)  "give me a snapshot of S..."
        reply: MIGRATE_SNAPSHOT {sid, snapshot, summary}
              or MIGRATE_FAIL {sid, reason}
              or RESIZE_FAIL {sid, error:"not_found"}      # backward-compat

    MIGRATE_ALLOCATE  (manager  -> target site)  "re-incarnate S... with this state"
        reply: ALLOCATE_OK {sid}     (reuses the existing ALLOCATE_OK)
              or ALLOCATE_FAIL {sid, reason}

    MIGRATE_COMPLETE   (manager  -> source site) "destroy your local copy of S..."
        reply: RELEASE_OK {sid}      (reuses the existing RELEASE_OK)

The manager orchestrates the whole dance from `_migrate_instance()` in
manager.py. The wire is unchanged in framing — only new `type` values.

Milestone 4 additions
---------------------
Optional HMAC-SHA256 authentication. If both endpoints are configured
with the same shared secret via --auth-key, every outbound message is
augmented with {"hmac": "<hex>", "_hmac_payload": "<canonical str>"} and
every inbound message is checked. Mismatches return {"type":"ERROR",
"error":"bad_hmac"} instead of being dispatched.

The canonical payload string is "<len>|<json-body-without-hmac-fields>".
"""
import hashlib
import hmac as _hmac_lib
import json
import socket
import struct

_HEADER = struct.Struct(">I")
_HMAC_FIELDS = ("hmac",)  # fields stripped before canonicalising


# ----------------------------------------------------------- framing ----
def send_msg(sock: socket.socket, obj: dict, auth_key: bytes = None) -> int:
    """Send one length-prefixed JSON message. Returns bytes written
    (header + payload), for message/byte counters. If `auth_key` is
    provided, attaches an HMAC-SHA256 over the canonical payload."""
    obj = _sign(obj, auth_key)
    payload = json.dumps(obj).encode("utf-8")
    sock.sendall(_HEADER.pack(len(payload)) + payload)
    return len(payload) + _HEADER.size


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed connection mid-message")
        buf.extend(chunk)
    return bytes(buf)


def recv_msg(sock: socket.socket, auth_key: bytes = None):
    """Read one length-prefixed JSON message. Returns (obj, bytes_read).
    If `auth_key` is provided and the message carries an `hmac` field,
    verifies it; on mismatch raises ValueError("bad_hmac")."""
    header = _recv_exact(sock, _HEADER.size)
    (length,) = _HEADER.unpack(header)
    payload = _recv_exact(sock, length)
    obj = json.loads(payload.decode("utf-8"))
    _verify(obj, auth_key)
    return obj, length + _HEADER.size


def request(host: str, port: int, obj: dict, timeout: float = 5.0,
            counters=None, auth_key: bytes = None):
    """Open a connection, send one message, read the one reply, close.
    This is the request/reply pattern used for every exchange in the
    protocol table (REGISTER, HEARTBEAT, REQUEST, RESIZE, ALLOCATE,
    INSTANCE_MIGRATE, MIGRATE_ALLOCATE, MIGRATE_COMPLETE, ...)."""
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sent = send_msg(sock, obj, auth_key=auth_key)
        if counters is not None:
            counters.sent(sent)
        reply, recvd = recv_msg(sock, auth_key=auth_key)
        if counters is not None:
            counters.recv(recvd)
        return reply


def serve_forever(host: str, port: int, handler, counters=None, stop_flag=None,
                  auth_key: bytes = None):
    """Generic request/reply TCP server. `handler(msg) -> reply_dict`.
    Each accepted connection handles exactly one request/reply exchange,
    matching the protocol's request/reply semantics. Returns the bound
    server socket so the caller can close it to stop serving.

    If `auth_key` is set, every inbound message is verified before the
    handler runs; verification failure produces an ERROR reply rather
    than dispatching the message.
    """
    import threading

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(64)

    def _client(conn):
        try:
            with conn:
                conn.settimeout(30)
                msg, recvd = recv_msg(conn, auth_key=auth_key)
                if counters is not None:
                    counters.recv(recvd)
                reply = handler(msg)
                sent = send_msg(conn, reply, auth_key=auth_key)
                if counters is not None:
                    counters.sent(sent)
        except ValueError as e:
            # bad_hmac -- don't reveal key material, just drop
            try:
                err = {"type": "ERROR", "error": str(e)}
                send_msg(conn, err, auth_key=auth_key)
            except OSError:
                pass
        except (ConnectionError, socket.timeout, OSError):
            pass

    def _accept_loop():
        while True:
            try:
                conn, _addr = srv.accept()
            except OSError:
                break  # socket was closed -> shut down
            threading.Thread(target=_client, args=(conn,), daemon=True).start()

    t = threading.Thread(target=_accept_loop, daemon=True)
    t.start()
    return srv


# ---------------------------------------------------------- HMAC helpers
def _canonical(obj: dict) -> bytes:
    """Canonical payload string for HMAC: "<len>|<json-without-hmac-fields>".
    Sorts keys for determinism; strips the `hmac` field itself."""
    body = {k: v for k, v in obj.items() if k not in _HMAC_FIELDS}
    s = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return f"{len(s)}|".encode("utf-8") + s.encode("utf-8")


def _sign(obj: dict, auth_key: bytes = None) -> dict:
    if auth_key is None:
        return obj
    out = dict(obj)
    payload = _canonical(out)
    digest = _hmac_lib.new(auth_key, payload, hashlib.sha256).hexdigest()
    out["hmac"] = digest
    return out


def _verify(obj: dict, auth_key: bytes = None) -> None:
    if auth_key is None:
        return
    sent_hmac = obj.get("hmac")
    if sent_hmac is None:
        raise ValueError("missing_hmac")
    payload = _canonical(obj)
    expected = _hmac_lib.new(auth_key, payload, hashlib.sha256).hexdigest()
    if not _hmac_lib.compare_digest(sent_hmac, expected):
        raise ValueError("bad_hmac")


def derive_key(passphrase: str) -> bytes:
    """Derive a 32-byte HMAC key from a passphrase using SHA-256.
    (Not a real KDF — keeps the demo honest without bringing in
    hashlib.pbkdf2_hmac's salt plumbing for what is essentially a
    shared-secret classroom demo.)"""
    return hashlib.sha256(("telerm|" + passphrase).encode("utf-8")).digest()
