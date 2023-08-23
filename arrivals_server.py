import jsonpickle
import threading
import time

from http.server import BaseHTTPRequestHandler, HTTPServer

PORT=8080

class ArrivalsServer:
    def __init__(self, get_arrivals):
      global _get_arrivals
      _get_arrivals = get_arrivals

    class Server(BaseHTTPRequestHandler):
        def do_GET(self):
            request_path = self.path
            arrivals = _get_arrivals()

            print("Request: ", request_path)

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write('{ "timestamp": "%d", data: "%s"' % (time.time(), jsonpickle.encode(arrivals).encode("utf-8")))
        
        do_POST = do_GET
        do_PUT = do_GET
        do_DELETE = do_GET

    def start(self) -> None:
        print('HTTP server listening on Port %s' % PORT)
        server = HTTPServer(('', PORT), self.Server)
        _thread = threading.Thread(target = server.serve_forever)
        _thread.start()
