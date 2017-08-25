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
        self._lock = threading.RLock()
        self._handler = set()

    def setURL(self, url):
        self._url = url
        self._url_set_ev.set()

    def getURL(self):
        self._url_set_ev.wait()
        return self._url

    def addClient(self, client):
        with self._lock:
            self._handler.add(client)

    def removeClient(self, client):
        with self._lock:
            self._handler.discard(client)

    def stopAll(self, wait=True):
        with self._lock:
            for hdlr in self._handler:
                hdlr.shutdown(wait=wait)
            self._handler = set() # remove all


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
        self._stopped = threading.Event()
        self._channel = self._connect()
        self._reader = threading.Thread(target=self._readloop)
        self._reader.start()

    def _connect(self):
        d = dict(chin=str(uuid.uuid4()))
        r = requests.post("{0}/connect".format(self._url), json=d)
        r.raise_for_status()
        obj = r.json()
        return obj['channel']

    def shutdown(self, wait=True):
        self._shutdown.set()
        self._stopped.wait()

    def _poll_receive(self, sequence, final=False):
        d = dict( sequence=sequence
                , channel=self._channel
                )
        if final:
            d['close'] = True
        r = requests.post("{0}/receive".format(self._url), json=d)
        r.raise_for_status()
        obj = r.json()
        rx = True
        if obj['channel'] != self._channel:
            logging.debug("Wrong channel received: {0}".format(obj['channel']))
            rx = False
        if obj['rx']:
            dec_data = base64.decodebytes(obj['data'].encode('ascii'))
            return dec_data, True
        else:
            return b'', False

    def _readloop(self):  # HTTP read requests (socket send)
        rseq = 0
        err_counter = 0
        final = False
        while not final:
            if self._shutdown.is_set():
                if not final:
                    logging.info("Poll receiver: prepare final request")
                    final = True
            try:
                data, rx = self._poll_receive(rseq, final=final)
                err_counter = 0
            except: # find exceptions
                logging.exception("Failed to receive data")
                rx = False
                err_counter += 1
                if err_counter > 5:
                    logging.debug("5 consecutive errors -> abort")
                    return
            if rx:
                if data == b'':
                    logging.info("Poll received close request")
                    return
                logging.debug("Poll receive: {0}".format(data))
                self._sock.sendall(data)
                rseq += 1
        logging.debug("Poll receiver: closed")

    def _forward_send(self, data, sequence):
        enc_data = base64.encodebytes(data).decode('ascii')
        d = dict( data=enc_data
                , sequence=sequence
                , channel=self._channel
                )
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
                return self._forward_send(data, wseq) # exit
            else:
                logging.debug("Forward send: {0}".format(data))
                try:
                    self._forward_send(data, wseq)
                    wseq += 1
                except: # find exceptions
                    logging.exception("Failed to send data")
                    return

    def process(self):
        # TODO: Think about this...
        self._writeloop()
        # TODO: Find a better solution
        self._shutdown.set()
        # TODO: trigger shutdown after a fixed timeout
        self._reader.join()
        self._stopped.set()


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
        ctrl.addClient(conn)
        self._LOG.debug("Start processing data from {0}".format(self.client_address))
        try:
            conn.process()
        except KeyboardInterrupt:
            conn.shutdown()
            self.on_close()
        ctrl.removeClient(conn)

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
    logging.getLogger("requests").setLevel(logging.WARNING)
    server = ThreadedTCPServer(('::', 7000), ClientInHandler)
    server.ctrl.setURL('http://127.0.0.1:7080')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Stopping clients")
        server.ctrl.stopAll()
        logging.info("Stopped")


if __name__ == '__main__':
    clientMain()
