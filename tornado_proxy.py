"""
Wrapper for tornado.httpserver.HTTPServer which understands the HAProxy
PROXY protocol for passing remote address information. For more information
about this protocol, see
    http://haproxy.1wt.eu/download/1.5/doc/proxy-protocol.txt

This version only works with Tornado 1.x
"""

try:
    import ssl
    ssl = ssl
except ImportError, e:
    ssl = None

import errno
import logging
import socket

import tornado
from tornado import iostream
from tornado.httpserver import HTTPServer, HTTPConnection

def get_proxy(stream, after):
    """Read PROXY information from the given IOStream, then call the given
    callback function.
    
    The callback will be passed a single argument, a tuple of the address as
    would've been returned from accept() (that is to say, ('a.b.c.d',
    80))"""
    def read_first_line(content, after):
        fields = content.split(" ")
        assert fields[0] == "PROXY", "Invalid PROXY line"
        assert fields[1] == "TCP4", "Only IPv4 is currently supported"
        source_address = fields[2]
        source_port = fields[4]
        after((source_address, source_port))
    stream.read_until("\r\n", functools.partial(read_first_line, after=after)

class _ProxyWrappedHTTPServerTornadoOne(HTTPServer):
    def __init__(self, *args, **kwargs):
        if kwargs.get('ssl_options', None) is not None:
            raise ValueError("Cannot use SSL with ProxyWrappedHTTPServer")
        return super(_ProxyWrappedHTTPServerTornadoOne, self).__init__(*args, **kwargs)

    def _handle_events(self, fd, events):
        while True:
            try:
                connection, address = self._socket.accept()
            except socket.error, e:
                if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    return
                raise
            assert self.ssl_options is not None, "SSL Not supported in wrapped servers"
            try:
                stream = iostream.IOStream(connection, io_loop=self.io_loop)
                get_proxy(stream, lambda address: HTTPConnection(stream, address, self.request_callback,
                         self.no_keep_alive, self.xheaders))
            except:
                logging.error("Error in connection callback", exc_info=True)

class _ProxyWrappedHTTPServerTornadoTwo(HTTPServer):
    def __init__(self, *args, **kwargs):
        if kwargs.get('ssl_options', None) is not None:
            raise ValueError("Cannot use SSL with ProxyWrappedHTTPServer")
        return super(_ProxyWrappedHTTPServerTornadoTwo, self).__init__(*args, **kwargs)

    def handle_stream(self, stream, _):
        get_proxy(stream, lambda address: HTTPConnection(stream, address, self.request_callback,
                self.no_keep_alive, self.xheaders))

if tornado.version_info < (2, 0, 0):
    ProxyWrappedHTTPServer = _ProxyWrappedHTTPServerTornadoOne
else:
    ProxyWrappedHTTPServer = _ProxyWrappedHTTPServerTornadoTwo
