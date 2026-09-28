import threading


class Counters:
    """Per-node message and byte counters (design doc 5.4: "Message and
    byte counts.")."""

    def __init__(self):
        self._lock = threading.Lock()
        self.messages_sent = 0
        self.messages_recv = 0
        self.bytes_sent = 0
        self.bytes_recv = 0

    def sent(self, nbytes: int):
        with self._lock:
            self.messages_sent += 1
            self.bytes_sent += nbytes

    def recv(self, nbytes: int):
        with self._lock:
            self.messages_recv += 1
            self.bytes_recv += nbytes

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "messages_sent": self.messages_sent,
                "messages_recv": self.messages_recv,
                "bytes_sent": self.bytes_sent,
                "bytes_recv": self.bytes_recv,
            }
