#!/usr/bin/python3

import sys
import cgi
import uuid
import json
import time  # just for stubs
import socket
import base64
import logging
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler


_DEFAULT_LOG_FORMAT = "%(name)s : %(threadName)s : %(levelname)s : %(message)s"
logging.basicConfig(stream=sys.stderr, format=_DEFAULT_LOG_FORMAT, level=logging.DEBUG)


class TunnelHandler(BaseHTTPRequestHandler):
    _LOG = logging.getLogger("TunnelHandler")
    BLA = 0

    def _reject(self, code=400):
        self.send_response(code)
        self.end_headers()

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
        self._json_send(d)

    def _send(self, obj):
        logging.debug("Forwarded TX: {0}".format(obj))
        self._json_send(dict(status='ok'))

    def _receive(self, obj):
        logging.debug("Polled RX: {0}".format(obj))
        d = dict( status='ok'
                , channel=obj['channel']
                , rx=False
                , data=base64.encodebytes(b'').decode('ascii')
                )
        time.sleep(0.5)
        TunnelHandler.BLA += 1
        if (TunnelHandler.BLA % 5) == 0:
            d['rx'] = True
            msg = 'BLA: {0}'.format(TunnelHandler.BLA)
            d['data'] = base64.encodebytes(msg.encode('utf-8')).decode('ascii')
        if TunnelHandler.BLA > 30:
            d['rx'] = True
            d['close'] = True
        self._json_send(d)

    def _communicate_main(self):
        if self.path == '/connect':
            call = self._connect
            self._LOG.debug("connect")
        elif self.path == '/send':
            call = self._send
            self._LOG.debug("send")
        elif self.path == '/receive':
            call = self._receive
            self._LOG.debug("receive")
        else:
            return self._reject()
        mime, params = cgi.parse_header(self.headers.get('content-type'))
        # Only JSON is allowed
        if mime != 'application/json':
            return self._reject()
        data_length = int(self.headers.get('content-length'))
        try:
            data = self.rfile.read(data_length)
            obj = json.loads(data.decode('utf-8'))
        except:  # TypeError or ...
            logging.exception("Couldn't parse JSON data")
            return self._reject()

        try:
            return call(obj)
        except KeyError:
            logging.exception("call failed {0}".format(self.path))
            return self._reject()

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


class HTTPServerIPv6(HTTPServer):
    address_family = socket.AF_INET6


def main():
    server = HTTPServerIPv6(('::', 7080), TunnelHandler)
    server.serve_forever()


if __name__ == '__main__':
    main()
