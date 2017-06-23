"""
Simple logging handler for sending gelf messages via TCP or UDP
Author: Stewart Rutledge <stew.rutledge AT gmail.com>
License: BSD I guess
"""
import logging
import socket
import json
import six
from .syslog import getSysLogLevelName
from .transport import ThreadedTCPTransport, ThreadedUDPTransport


class gelfHandler(logging.Handler):
    '''
    TODO
    '''

    def __init__(self, **kw):
        '''
        TODO
        '''
        self.proto = kw.get('proto', 'UDP')
        self.host = kw.get('host', 'localhost')
        self.port = kw.get('port', None)
        self.fullInfo = kw.get('fullInfo', False)
        self.facility = kw.get('facility', None)
        self.fromHost = kw.get('fromHost', socket.getfqdn())
        self.globalProps = kw.get('gelfProps', {})
        self.tls = kw.get('tls', False)
        self._transport = None
        if self.proto == 'UDP':
            self.connectUDPSocket()
        if self.proto == 'TCP':
            self.connectTCPSocket()
        logging.Handler.__init__(self)

    def connectUDPSocket(self):
        '''
        TODO
        '''
        url = 'udp://%s:%d/' % (self.host, int(self.port or 12202))
        self._transport = ThreadedUDPTransport(url)

    def connectTCPSocket(self):
        '''
        TODO
        '''
        url = 'tcp://%s:%d/' % (self.host, int(self.port or 12201))
        self._transport = ThreadedTCPTransport(url)

    def buildMessage(self, record, **_):
        '''
        TODO
        '''
        recordDict = record.__dict__
        msgDict = {}
        msgDict['version'] = '1.1'
        msgDict['host'] = self.fromHost
        msgDict['timestamp'] = recordDict['created']
        msgDict['level'] = getSysLogLevelName(recordDict['levelname'])
        msgDict['short_message'] = recordDict['message']
        if recordDict.get('exc_text', None):
            msgDict['full_message'] = recordDict['exc_text']
        if self.fullInfo is True:
            msgDict['function'] = recordDict['funcName']
            msgDict['line'] = recordDict['lineno']
            msgDict['module'] = recordDict['module']
            msgDict['process_id'] = recordDict['process']
            msgDict['process_name'] = recordDict['processName']
            msgDict['thread_id'] = recordDict['thread']
            msgDict['thread_name'] = recordDict['threadName']
        if self.facility is not None:
            msgDict['facility'] = self.facility
        elif self.facility is None:
            msgDict['facility'] = recordDict['name']
        extra_props = recordDict.get('gelfProps', None)
        if isinstance(extra_props, dict):
            for key, value in extra_props.items():
                msgDict['_' + key] = value
        global_props = self.globalProps
        if isinstance(global_props, dict):
            for key, value in global_props.items():
                msgDict['_' + key] = value
        return msgDict

    def dumps(self, data):
        '''
        TODO
        '''
        return json.dumps(data)

    def formatMessage(self, msgDict):
        '''
        TODO
        '''
        if self.proto == 'UDP':
            msg = self.dumps(msgDict)
        if self.proto == 'TCP':
            msg = self.dumps(msgDict) + '\0'
        return msg

    def emit(self, record, **_):
        '''
        TODO
        '''
        msgDict = None
        try:
            self.format(record)
            msgDict = self.buildMessage(record)
            msg = self.formatMessage(msgDict)
            if isinstance(msg, six.text_type):
                try:
                    msg = msg.encode('utf-8')
                except:
                    msg = msg.encode('latin1')
        except UnicodeEncodeError as err:
            err.data = msgDict
            self.emit_failure(err, level=logging.WARNING)
        except ValueError as err:
            err.data = msgDict
            self.emit_failure(err)
        except Exception as err:
            err.data = msgDict
            self.emit_failure(err)
            raise
        else:
            self._transport.async_send(
                msg, headers=None,
                success_cb=self.emit_success,
                failure_cb=self.emit_failure)

    def emit_success(self):
        '''
        TODO
        '''
        pass

    def emit_failure(self, e, **_):
        '''
        TODO
        '''
        print(repr(e))

    def close(self):
        '''
        TODO
        '''
        if self.proto == 'TCP':
            self._transport.sock.close()
