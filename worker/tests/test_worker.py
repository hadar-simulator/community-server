import pickle
import threading
import unittest
import hadar as hd
from http.server import BaseHTTPRequestHandler, HTTPServer

from worker.worker import compute, Client, JobDTO


class MockSchedulerServer(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])

        path = self.path.split('/')
        assert path[1] == 'job'

        id = path[2]

        data = self.rfile.read(content_length)
        assert data is not None

        self.send_response(200)
        self.send_header('Content-Length', str(len(id)))
        self.end_headers()
        self.wfile.write(id.encode())

    def do_GET(self):
        assert '/job/next' == self.path

        study = hd.Study(horizon=1)\
            .network()\
                .node('a')\
                    .consumption(name='load', cost=1000, quantity=10)\
                    .production(name='prod', cost=10, quantity=10)\
            .build()

        job = JobDTO(study=study, id='123', version='1', created=147, status='QUEUED')
        data = pickle.dumps(job)

        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def handle_twice(handle_request):
    handle_request()  # one for GET /job/next
    handle_request()  # second for POST /job/123


class TestWorker(unittest.TestCase):
    def test_compute(self):
        # Start server
        httpd = HTTPServer(('localhost', 6964), MockSchedulerServer)
        server = threading.Thread(None, handle_twice, None, (httpd.handle_request,))
        server.start()

        client = Client('http://localhost:6964')
        id = compute(client)
        self.assertEqual('123', id)