"""
Simple logging handler for sending gelf messages via TCP or UDP
Author: Stewart Rutledge <stew.rutledge AT gmail.com>
License: BSD I guess
"""
import logging
from socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM, getfqdn
from ssl import *
from json import dumps
from idlib import compress
from logging.handlers import SysLogHandler
from transport import ThreadedTCPTransport, ThreadedUDPTransport

def addSysLogLevelName(level, levelName):
    SysLogHandler.priority_names[levelName] = level

def getSysLogLevelName(level):
    if isinstance(level, (bytes, str, unicode)):
	result = SysLogHandler.priority_names[level.lower()]
	return result
    elif isinstance(level, (int, long)):
	results = [k for (k, v) in SysLogHandler.priority_names.items() if v == level]
	results.sort(key=lambda x: len(x))
	return results[-1].upper()
    else:
        raise ValueError("unknown syslog level name %s %s" % (type(level), repr(level)))

class handler(logging.Handler):

    def __init__(self, **kw):
        self.proto = kw.get('proto', 'UDP')
        self.host = kw.get('host', 'localhost')
        self.port = kw.get('port', None)
        self.fullInfo = kw.get('fullInfo', False)
        self.facility = kw.get('facility', None)
        self.fromHost = kw.get('fromHost', getfqdn())
        self.tls = kw.get('tls', False)
        if self.proto == 'UDP':
            self.connectUDPSocket()
        if self.proto == 'TCP':
            self.connectTCPSocket()
        logging.Handler.__init__(self)

    def connectUDPSocket(self):
        url = 'udp://%s:%d/' % (self.host, int(self.port or 12202))
        self._transport = ThreadedUDPTransport(url)

    def connectTCPSocket(self):
        url = 'tcp://%s:%d/' % (self.host, int(self.port or 12201))
        self._transport = ThreadedTCPTransport(url)

    def buildMessage(self, record, **kwargs):
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
            for k, v in extra_props.iteritems():
                msgDict['_' + k] = v
        return msgDict

    def formatMessage(self, msgDict):
        if self.proto == 'UDP':
            msg = dumps(msgDict)
        if self.proto == 'TCP':
            msg = dumps(msgDict) + '\0'
        return msg

    def sendOverTCP(self, msg):
        totalsent = 0
        while totalsent < len(msg):
            sent = self.sock.send(msg[totalsent:])
            if sent == 0:
                raise IOError("socket connection broken")
            totalsent = totalsent + sent

    def emit(self, record, **kwargs):
        try:
            self.format(record)
            msgDict = self.buildMessage(record, **kwargs)
            msg = self.formatMessage(msgDict)
        except UnicodeEncodeError, e:
            print "%s in %s" % (e, msgDict)
        except Exception, e:
            print "%s in %s" % (e, msgDict)

        self._transport.async_send(
            msg, headers=None,
            success_cb=self.emit_success,
            failure_cb=self.emit_failure)

    def emit_success(self):
        pass
    
    def emit_failure(self, e):
        raise e
    
    def close(self):
        if self.proto == 'TCP':
            self._transport.sock.close()
