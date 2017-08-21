#!/usr/bin/python3

import sys
import uuid
import queue
import socket
import base64
import logging
import threading
import socketserver
import http.client
import requests

_DEFAULT_LOG_FORMAT = "%(name)s : %(threadName)s : %(levelname)s : %(message)s"
logging.basicConfig(stream=sys.stderr, format=_DEFAULT_LOG_FORMAT, level=logging.DEBUG)


class ClientController(object):
    def __init__(self):
        self._url = None
        self._url_set_ev = threading.Event()

    def setURL(self, url):
        self._url = url
        self._url_set_ev.set()

    def getURL(self):
        self._url_set_ev.wait()
        return self._url


class Client(object):
    RX_SIZE = 1024
    TIMEOUT = 0.5

    def __init__(self, url, sock):
        self._url = url
        self._sock = sock # shared
        self._sock.settimeout(self.TIMEOUT)
        self._rq = queue.Queue()
        self._wq = queue.Queue()
        self._shutdown = threading.Event()
        self._channel = self._connect()
        self._reader = threading.Thread(target=self._readloop)
        self._writer = threading.Thread(target=self._writeloop)

    def _connect(self):
        d = dict(chin=str(uuid.uuid4()))
        r = requests.post("{0}/connect".format(self._url), json=d)
        r.raise_for_status()
        obj = r.json()
        return obj['channel']

    def shutdown(self):
        self._shutdown.set()

    def write(self, data):
        d = dict(data=data, seq=self._wseq)
        self._wseq += 1
        self._wg.put(d)

    def read(self):
        return self._rq.get()

    def _readloop(self):  # HTTP read requests (socket send)
        rseq = 0
        while not self._shutdown.is_set():
            pass

    def _forward_send(self, data, sequence):
        enc_data = base64.encodebytes(data).decode('ascii')
        d = dict(data=enc_data, sequence=sequence)
        r = requests.post("{0}/send".format(self._url), json=d)
        r.raise_for_status()

    def _writeloop(self): # HTTP write requests (socket recv)
        wseq = 0
        while not self._shutdown.is_set():
            try:
                data = self._sock.recv(self.RX_SIZE)
            except socket.timeout:
                continue
            if data == b'':
                logging.info("Close request from client socket")
                return
            else:
                logging.debug("Got data: {0}".format(data))
                try:
                    self._forward_send(data, wseq)
                    wseq += 1
                except: # find exceptions
                    logging.exception("Failed to send data")

    def process(self):
        #TODO: Think about this...
        self._writeloop()
        # JOIN other thread


class ClientInHandler(socketserver.BaseRequestHandler):
    RX_SIZE = 1024
    _LOG = logging.getLogger('ClientIN')

    def setup(self):
        self._LOG.debug("Prepare request")

    def handle(self):
        try:
            ctrl = self.server.ctrl
        except AttributeError:
            logging.exception("Missing controller object")
            return self.on_close()
        self._LOG.info("Connect to server: {0}".format(ctrl.getURL()))
        conn = Client(ctrl.getURL(), self.request)
        self._LOG.debug("Start processing data from {0}".format(self.client_address))
        try:
            conn.process()
        except KeyboardInterrupt:
            conn.shutdown()
            self.request.close()
#        while True:
#            data = self.request.recv(self.RX_SIZE)
#            if data == b'':
#                return self.on_close()
#            else:
#                self._LOG.debug("RX (size={1}): {0}".format(data, len(data)))

    def on_close(self):
        self.request.close()
        self._LOG.debug("Close connection to {0}".format(self.client_address))

    def finish(self):
        self._LOG.debug("Finish request")


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    address_family = socket.AF_INET6
    allow_reuse_address = True
    ctrl = ClientController()


def clientMain():
    server = ThreadedTCPServer(('::', 7000), ClientInHandler)
    server.ctrl.setURL('http://127.0.0.1:7080')
    server.serve_forever()


if __name__ == '__main__':
    clientMain()
