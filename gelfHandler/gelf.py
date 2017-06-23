import logging
import six
import socket
import ssl
from socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM, getfqdn
from json import dumps
from zlib import compress
from .syslog import getSysLogLevelName
from .transport import ThreadedTCPTransport, ThreadedUDPTransport


class GelfHandler(logging.Handler):
    """
    Simple logging handler for sending gelf messages via TCP or UDP
    Author: Stewart Rutledge <stew.rutledge AT gmail.com>
    License: BSD
    """
    def __init__(self, protocol='UDP', host='localhost', port=None, **kw):
        """
        Simple
        :param protocol: Which protocol to use (TCP or UDP) [Default: UDP]
        :param host: Host to send logs to [Default: localhost]
        :param port: Port [Default: 12201 UDP/12202 TCP
        :param full_info: Include function name, pid and process [Default: False]
        :param facility: Logging facility [Default: None]
        :param from_host: Host name in the log message [Default: FQDN]
        :param tls: Use a tls connection [Default: False]
        :type protocol str
        :type host: str
        :type port: int
        :type full_info: bool
        :type facility: str
        :type from_host: str
        :type tl: bool
        """
        if not protocol.lower() in ['tcp', 'udp']:
            raise ValueError('Protocol must be either TCP or UDP')
        self.protocol = kw.get('proto', protocol)
        self.host = host
        self.port = port
        self.full_info = kw.get('full_info', kw.get('fullInfo', False))
        self.facility = kw.get('facility', None)
        self.from_host = kw.get('from_host', kw.get('fromHost', getfqdn()))
        self.tls = kw.get('tls', False)
        self.global_props = kw.get('gelf_props', kw.get('gelfProps', {}))
        self.application = kw.get('application', None)
        self._transport = None
        if self.protocol.lower() == 'udp':
            self._connect_udp_socket()
        if self.protocol.lower() == 'tcp':
            self._connect_tcp_socket()
        logging.Handler.__init__(self)

    # def _connect_udp_socket_old(self):
    #     if self.port is None:
    #         self.port = 12202
    #     self.sock = socket(AF_INET, SOCK_DGRAM)
    def _connect_udp_socket(self):
        url = 'udp://%s:%d/' % (self.host, int(self.port or 12202))
        self._transport = ThreadedUDPTransport(url)

    # def _connect_tcp_socket_old(self):
    #     if self.port is None:
    #         self.port = 12201
    #     self.sock = socket(AF_INET, SOCK_STREAM)
    #     if self.tls:
    #         self.sock = ssl.wrap_socket(self.sock, ssl_version=ssl.PROTOCOL_TLSv1, cert_reqs=ssl.CERT_NONE)
    #     try:
    #         self.sock.connect((self.host, int(self.port)))
    #     except IOError as e:
    #         raise RuntimeError('Could not connect via TCP: %s' % e)
    def _connect_tcp_socket(self):
        url = 'tcp://%s:%d/' % (self.host, int(self.port or 12201))
        self._transport = ThreadedTCPTransport(url)

    # def getLevelNoOld(self, level):
    #     levelsDict = {
    #         'WARNING': 4,
    #         'INFO': 6,
    #         'DEBUG': 7,
    #         'ERROR': 3,
    #         'CRITICAL': 9
    #     }
    #     try:
    #         return(levelsDict[level])
    #     except:
    #         raise('Could not determine level number')
    def getLevelNo(self, level):
        return getSysLogLevelName(level)

    def _build_message(self, record, **kwargs):
        record_dict = record.__dict__
        msg_dict = {}
        msg_dict['version'] = '1.1'
        msg_dict['timestamp'] = record_dict['created']
        msg_dict['level'] = self.getLevelNo(record_dict['levelname'])
        msg_dict['short_message'] = record_dict['msg']
        msg_dict['host'] = self.from_host
        if self.application:
            msg_dict['application'] = record_dict['application']
        if self.full_info is True:
            # msg_dict['pid'] = record_dict['process']
            # msg_dict['processName'] = record_dict['processName']
            # msg_dict['funcName'] = record_dict['funcName']
            msg_dict['function'] = record_dict['funcName']
            msg_dict['line'] = record_dict['lineno']
            msg_dict['module'] = record_dict['module']
            msg_dict['process_id'] = record_dict['process']
            msg_dict['process_name'] = record_dict['processName']
            msg_dict['thread_id'] = record_dict['thread']
            msg_dict['thread_name'] = record_dict['threadName']
        if record_dict.get('exc_text', None):
            msg_dict['full_message'] = record_dict['exc_text']
        if self.facility is not None:
            msg_dict['facility'] = self.facility
        elif self.facility is None:
            msg_dict['facility'] = record_dict['name']
        extra_props = record_dict.get('gelf_props', record_dict.get('gelfProps', None))
        if isinstance(extra_props, dict):
            for key, value in extra_props.items():
                msg_dict['_' + key] = value
        global_props = self.global_props
        if isinstance(global_props, dict):
            for key, value in global_props.items():
                msg_dict['_' + key] = value
        return msg_dict

    def dumps(self, data):
        '''
        TODO
        '''
        return dumps(data)

    def _format_message(self, msg_dict):
        '''
        TODO
        '''
        if self.protocol.lower() == 'udp':
            msg = self.dumps(msg_dict)
        elif self.protocol.lower() == 'tcp':
            msg = self.dumps(msg_dict) + '\0'
        if isinstance(msg, six.text_type):
            try:
                msg = msg.encode('utf-8')
            except:
                msg = msg.encode('latin1')
        return msg

    # def _emit_tcp_old(self, msg):
    #     totalsent = 0
    #     while totalsent < len(msg):
    #         sent = self.sock.send(msg[totalsent:].encode())
    #         if sent == 0:
    #             raise IOError("socket connection broken")
    #         totalsent = totalsent + sent
    def _emit_tcp(self, msg):
        self._transport.async_send(
            msg, headers=None,
            success_cb=self.emit_success,
            failure_cb=self.emit_failure)

    # def _emit_udp_old(self, compressed_msg):
    #     self.sock.sendto(compressed_msg, (self.host, self.port))
    def _emit_udp(self, compressed_msg):
        self._transport.async_send(
            compressed_msg, headers=None,
            success_cb=self.emit_success,
            failure_cb=self.emit_failure)

    # def emit_old(self, record, **kwargs):
    #     msg_dict = self._build_message(record, **kwargs)
    #     if self.protocol.lower() == 'udp':
    #         compressed_msg = compress(dumps(msg_dict).encode())
    #         self._emit_udp(compressed_msg=compressed_msg)
    #     if self.protocol.lower() == 'tcp':
    #         msg = dumps(msg_dict) + '\0'
    #         try:
    #             self._emit_tcp(msg)
    #         except IOError:
    #             try:
    #                 self.sock.close()
    #                 self._connect_tcp_socket()
    #                 self._emit_tcp(msg)
    #             except IOError as e:
    #                 raise RuntimeError('Could not connect via TCP: %s' % e)
    def emit(self, record, **kwargs):
        '''
        Send logs to Graylog using GELF formatted messages.
        '''
        msg_dict = None
        msg = None
        try:
            self.format(record)
            msg_dict = self._build_message(record, **kwargs)
            msg = self._format_message(msg_dict)
        except UnicodeEncodeError as err:
            err.data = msg_dict
            self.emit_failure(err, level=logging.WARNING)
        except ValueError as err:
            err.data = msg_dict
            self.emit_failure(err)
        except Exception as err:
            err.data = msg_dict
            self.emit_failure(err)
            raise
        else:
            # if self.protocol.lower() == 'udp':
            #     compressed_msg = compress(dumps(msg_dict).encode())
            #     self._emit_udp(compressed_msg=compressed_msg)
            # elif self.protocol.lower() == 'tcp':
            #     self._emit_tcp(msg)
            self._transport.async_send(
                msg, headers=None,
                success_cb=self.emit_success,
                failure_cb=self.emit_failure)

    def close(self):
        '''
        Close transport
        '''
        if self.protocol.lower() == 'tcp':
            self._transport.sock.close()

    def emit_success(self):
        '''
        Callback for success
        '''
        pass

    def emit_failure(self, err, **_):
        '''
        Callback for failure
        '''
        print(repr(err))
