#!/usr/bin/env python3
"""Fake ClickHouse HTTP server for chctl replica gc smoke testing.

Responds to the driver's ping (SELECT 1) and to the stale-replica
detection query with a canned system.replicas answer: two stale readers
plus one stale writer (the writer must survive the pattern gate).
"""
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _respond(self, payload):
        data = payload.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/tab-separated-values; charset=UTF-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        # The mailru driver sends the query as a chunked body; decode the
        # chunks before matching (BaseHTTPRequestHandler doesn't).
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            body = self._read_chunked()
        else:
            length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(length).decode() if length else ""
        print(f"POST {self.path!r} body={body[:60]!r}", file=sys.stderr)
        if "SYSTEM DROP" in body:
            self._respond("ok\n")
        elif "system.replicas" in body or "system.replicas" in self.path:
            self._respond("host_name\nString\nreader-3\nreader-9\nwriter-0\n")
        else:
            self._respond("1\n")

    def _read_chunked(self):
        chunks = []
        while True:
            size = int(self.rfile.readline().strip(), 16)
            if size == 0:
                self.rfile.readline()
                break
            chunks.append(self.rfile.read(size).decode())
            self.rfile.readline()
        return "".join(chunks)

    def do_GET(self):
        print(f"GET {self.path!r}", file=sys.stderr)
        if "system.replicas" in self.path:
            self._respond("host_name\nString\nreader-3\nreader-9\nwriter-0\n")
        else:
            self._respond("1\n")

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    from http.server import ThreadingHTTPServer
    ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
