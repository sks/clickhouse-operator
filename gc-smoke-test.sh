#!/bin/bash
set -u
FAKE_PORT=18124

python3 - "$FAKE_PORT" <<'PYEOF' 2>server.log &
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

class H(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def handle_one_request(self):
        try:
            BaseHTTPRequestHandler.handle_one_request(self)
        except Exception as e:
            sys.stderr.write(f"handler error: {e}\n")
    def _respond(self, payload):
        data = payload.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/tab-separated-values; charset=UTF-8')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0) or 0)
        body = self.rfile.read(length).decode() if length else ''
        url_query = self.path
        combined = body + ' ' + url_query
        sys.stderr.write(f"POST path={self.path!r} body={body[:80]!r}\n")
        if 'system.replicas' in combined:
            self._respond("host_name\nString\nreader-3\nreader-9\nwriter-0\n")
        else:
            self._respond("1\n")
    def do_GET(self):
        sys.stderr.write(f"GET path={self.path!r}\n")
        if 'system.replicas' in self.path:
            self._respond("host_name\nString\nreader-3\nreader-9\nwriter-0\n")
        else:
            self._respond("1\n")
    def log_message(self, *a): pass

HTTPServer(('127.0.0.1', int(sys.argv[1])), H).serve_forever()
PYEOF
FAKE_PID=$!
sleep 1

SPEC=docs/chctl-example-chi.yaml

echo "=== gc run ==="
./chctl-gc-test replica gc \
  -spec "$SPEC" -cluster logexplorer -on 0-0 \
  -host 0-0=127.0.0.1 -port $FAKE_PORT -user default
echo "exit=$?"

kill $FAKE_PID 2>/dev/null
echo "=== server log ==="
cat server.log | head -20
