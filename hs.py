#!/usr/bin/python3

import sys
import cgi
import uuid
import json
import time  # just for stubs
import queue
import socket
import base64
import logging
import argparse
import threading
import socketserver
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler


_DEFAULT_LOG_FORMAT = "%(name)s : %(threadName)s : %(levelname)s : %(message)s"
logging.basicConfig(stream=sys.stderr, format=_DEFAULT_LOG_FORMAT, level=logging.DEBUG)


class ForwardConnection(object):
    _LOG = logging.getLogger('fwd')
    _TIMEOUT = 0.5
    RX_SIZE = 1024

    def __init__(self, channel, host, port, family):
        self._channel = channel
        self._lock = threading.RLock()
        self._rx_closed = False
        self._tx_closed = False
        self._close_rq = threading.Event()
        self._msg = []
        self._addr = (host, port)
        self._addr_family = family
        self._conn = self._connect()
        self._tx_queue = queue.Queue()
#        self._tx_lock = threading.RLock()
#        self._tx_seq = 0
        self._tx_thread = threading.Thread(target=self._tx_main)
        self._rx_queue = queue.Queue()
        self._rx_thread = threading.Thread(target=self._rx_main)
        self._tx_thread.start()
        self._rx_thread.start()

    def _connect(self):
        conn = socket.socket(self._addr_family, socket.SOCK_STREAM, 0)
        try:
            conn.connect(self._addr)
        except socket.error:
            self._LOG.exception("Couldn't connect: {0}".format(self._addr))
            raise
        conn.settimeout(self._TIMEOUT)
        return conn

    def _shallClose(self):
        return self._close_rq.is_set()

    def _rx_main(self):
        try:
            self._do_rx()
        finally:
            with self._lock:
                self._rx_closed = True
            self._LOG.debug("Closed RX thread")

    def _do_rx(self):
        while not self._shallClose():
            try:
                data = self._conn.recv(self.RX_SIZE)
                self._rx_queue.put(data)
                self._LOG.debug("Put RX data into queue: {0}".format(data))
                if data == b'':
                    return  # quit
            except socket.timeout:
                continue

    def get(self, timeout=0.5, max_retries=10):
        all_data = b''
        kwargs = dict(block=True, timeout=timeout)  # first request
        try:
            for i in range(max_retries):
                data = self._rx_queue.get(**kwargs)
                self._LOG.debug("Fetch RX data from queue: {0}".format(data))
                if data == b'':
                    return all_data, True
                else:
                    all_data += data
                    # Don't block for any futher call
                    kwargs = dict(block=False, timeout=None)
        except queue.Empty:
            pass
        # no more data or runs reached
        return all_data, False

    def put(self, data, seq):
        # UGLY: Ignore sequence numbers for now
        # TODO: To many messages with wrong sequence -> close
#        with self._tx_lock:
        self._tx_queue.put(dict(data=data, sequence=seq))

    def _tx_main(self):
        try:
            self._do_tx()
        finally:
            with self._lock:
                self._tx_closed = True
            self._LOG.debug("Closed TX thread")

    def _do_tx(self):
        while not self._shallClose():
            try:
                data = self._tx_queue.get(timeout=self._TIMEOUT)
                if data['data'] == b'':
                    return
                else:
                    logging.debug("Forwarding data: {0}".format(data))
                    self._conn.sendall(data['data'])
            except queue.Empty:
                continue
            # TODO: Catch errors of sendall

    def shutdown(self):
        self._close_rq.set()

    def isClosed(self):
        with self._lock:
            if self._rx_closed and self._tx_closed:
                return True
        return False

    def join(self, force_shutdown=False):
        if force_shutdown:
            self.shutdown()
        self._tx_thread.join()
        self._rx_thread.join()
        self._conn.close()


class InvalidChannelError(Exception):
    def __init__(self, channel, desc, op):
        msg = "Invalid channel  '{0}': {1} ({2})".format(channel, desc, op)
        Exception.__init__(self, msg)
        self.channel = channel
        self.desc = desc
        self.operation = op


class ConnectionManager(object):

    def __init__(self):
        self._lock = threading.RLock()
        self._closing = False
        self._conn = dict()
        self._host = None
        self._port = None
        self._address_family = None
        self._configured = False

    def configure(self, host, port, family=socket.AF_INET6):
        if self._configured:
            raise RuntimeError("Already configured")
        elif (host is None) or (port is None):
            msg = "Illegal configuration: host={0}, port={1}".format(host, port)
            raise RuntimeError(msg)
        elif family not in (socket.AF_INET, socket.AF_INET6):
            raise RuntimeError("Invalid address family: {0}".format(family))
        else:
            self._host = host
            self._port = port
            self._address_family = family
            self._configured = True

    def _garbageCollect(self):
        remove_items = dict()
        with self._lock:
            for k in self._conn.keys():
                conn = self._conn[k]
                if conn.isClosed():
                    remove_items[k] = conn
            for k in remove_items.keys():
                del self._conn[k]
        for k,v in remove_items.items():
            v.join()

    def addChannel(self, channel):
        assert self._configured, "Must be configured before usage"
        self._garbageCollect()
        with self._lock:
            if self._closing:
                raise RuntimeError("Server is shutting down")
            if channel in self._conn:
                raise InvalidChannelError(channel, "already exists", 'addChannel')
            else:
                self._conn[channel] = ForwardConnection \
                        (channel, self._host, self._port, self._address_family)

    def getConnection(self, channel):
        with self._lock:
            conn = self._conn.get(channel, None)
        if conn is None:
            raise InvalidChannelError(channel, "doesn't exist", 'getConnection')
        else:
            return conn

    def removeChannel(self, channel):
        try:
            conn = self.getConnection(channel)
        except InvalidChannelError:
            logging.exception("Channel '{0}' has already been removed".format(channel))
        else:
            conn.join()

    def forceShutdown(self):
        with self._lock:
            self._closing = True  # don't accept any further connections
            for k,v in self._conn.items():
                logging.debug("Forcing channel {0} to close".format(k))
                v.join(force_shutdown=True)
                logging.debug("Killed channel {0}".format(k))
        logging.info("All channels have been closed")


class TunnelHandler(BaseHTTPRequestHandler):
    _LOG = logging.getLogger("TunnelHandler")
    _POLL_RX_TIMEOUT = 10.0
    _REQ_CONNECT = set(('chin',))
    _REQ_SEND = set(('channel', 'data', 'sequence'))
    _REQ_RECV = set(('channel',))

    def _reject(self, code=400, obj=None):
        self.send_response(code)
        if obj is None:
            self.end_headers()
        else:
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            data = json.dumps(obj).encode('utf-8')
            self.wfile.write(data)

    def _json_send(self, obj):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        data = json.dumps(obj).encode('utf-8')
        self.wfile.write(data)

    def _connect(self, obj):
        ch_srv = str(uuid.uuid4())
        ch = "{0}{1}".format(obj['chin'], ch_srv)
        d = dict(channel=ch)
        try:
            self.server.cm.addChannel(ch)
            return self._json_send(d)
        except InvalidChannelError as e:
            d['error'] = str(e)
            return self._reject(obj=d)

    def _send(self, obj):
        self._LOG.debug("Forwarded TX: {0}".format(obj))
        conn = self.server.cm.getConnection(obj['channel'])
        data = base64.decodebytes(obj['data'].encode('ascii'))
        conn.put(data, obj['sequence'])
        self._json_send(dict(status='ok'))

    def _receive(self, obj):
        self._LOG.debug("Polled RX: {0}".format(obj))
        conn = self.server.cm.getConnection(obj['channel'])
        d = dict( status = 'ok'
                , channel = obj['channel']
                , rx = True
                )
        data, closed = conn.get(timeout=self._POLL_RX_TIMEOUT)
        d['data'] = base64.encodebytes(data).decode('ascii')
        d['close'] = closed
        if closed:
            self.server.cm.removeChannel(obj['channel'])
            self._LOG.debug("Close channel: {0}".format(obj['channel']))
        elif data == b'':
            d['rx'] = False
        self._LOG.debug("Polled RX - received {0}".format(d))
        self._json_send(d)

    def _communicate_main(self):
        if self.path == '/connect':
            call = self._connect
            self._LOG.debug("connect")
            check_keys = self._REQ_CONNECT
        elif self.path == '/send':
            call = self._send
            self._LOG.debug("send")
            check_keys = self._REQ_SEND
        elif self.path == '/receive':
            call = self._receive
            self._LOG.debug("receive")
            check_keys = self._REQ_RECV
        else:
            return self._reject()

        mime, params = cgi.parse_header(self.headers.get('content-type'))
        # Only JSON is allowed
        if mime != 'application/json':
            d = dict(error="Invalid MIME type: {0}".format(mime))
            return self._reject(obj=d)
        data_length = int(self.headers.get('content-length'))
        try:
            data = self.rfile.read(data_length)
            obj = json.loads(data.decode('utf-8'))
        except:  # TypeError or ...
            self._LOG.exception("Couldn't parse JSON data")
            return self._reject(obj=dict(error='Invalid data type'))

        # validate obj
        if not check_keys.issubset(obj.keys()):
            missing = check_keys.difference(obj.keys())
            msg = "Missing key in request: {0}".format(missing)
            self._LOG.error("{0}; path={1}, obj={2}".format(msg, self.path, obj))
            return self._reject(obj=dict(error=msg))

        try:
            return call(obj)
        except KeyError:
            self._LOG.exception("call failed {0}".format(self.path))
            return self._reject()
        except InvalidChannelError as e:
            return self._reject(obj=dict(error=str(e)))
        except ConnectionError as e:
            self._LOG.exception("Connection error: {0}".format(e))
            return self._reject(obj=dict(error=str(e)))
        except Exception as e:
            self._LOG.exception("Unexpected error: {0}".format(e))
            return self._reject(obj=dict(error='Unexpected error'))

    def do_POST(self):
        return self._communicate_main()

    def do_GET(self):
        return self._communicate_main()

    def do_HEAD(self):
        return self._reject()

    def do_DELETE(self):
        return self._reject()

    def log_message(self, format, *args):
        return


class HTTPServerCM(socketserver.ThreadingMixIn, HTTPServer):
    address_family = socket.AF_INET6
    cm = ConnectionManager()


def _parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument( '-6'
                       , dest = 'address_family'
                       , const = socket.AF_INET6
                       , action = 'store_const'
                       )
    parser.add_argument( '-4'
                       , dest = 'address_family'
                       , const = socket.AF_INET
                       , action = 'store_const'
                       )
    parser.add_argument( 'host'
                       , help = "hostname or IP address to connect to"
                       )
    parser.add_argument( 'port'
                       , type = int
                       , help = "port to forward to"
                       )
    parser.add_argument( '--http-port', '-P'
                       , type = int
                       , default = 8080
                       , help = "HTTP port listing on"
                       )
    parser.add_argument( '--http-host', '-H'
                       , default = '::1'
                       , help = "HTTP hostname or IP"
                       )
    parser.add_argument( '--http-ipv4'
                       , dest = 'http_addr_family'
                       , const = socket.AF_INET
                       , action = 'store_const'
                       )
    parser.add_argument( '--http-ipv6'
                       , dest = 'http_addr_family'
                       , const = socket.AF_INET6
                       , action = 'store_const'
                       )
    parser.set_defaults( address_family = socket.AF_INET6
                       , http_addr_family = socket.AF_INET6
                       )
    return parser.parse_args()


def main():
    args = _parseArguments()
    http_server_class = HTTPServerCM
    http_server_class.address_family = args.http_addr_family
    server = http_server_class((args.http_host, args.http_port), TunnelHandler)
    server.cm.configure(args.host, args.port, args.address_family)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        logging.info("Stopping all connections")
        server.cm.forceShutdown()
        logging.info("All connections  have been closed")


if __name__ == '__main__':
    main()
