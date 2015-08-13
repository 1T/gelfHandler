# Inspired by github.com/getsentry/raven-python.git
# (c) 2010-2012 by the Sentry Team
# License: BSD

import atexit
import logging
import os
import six
import socket
import ssl
import threading
import time

try:
    from queue import Queue
except ImportError:
    from Queue import Queue  # NOQA

DEFAULT_TIMEOUT = 10

logger = logging.getLogger(__name__)


class Transport(object):
    async = False
    scheme = []

    def send(self, data, headers):
        raise NotImplementedError


class AsyncTransport(Transport):
    async = True

    def async_send(self, data, headers, success_cb, error_cb):
        raise NotImplementedError


class TCPTransport(Transport):
    scheme = ['sync+tcp']

    def __init__(self, url, timeout=DEFAULT_TIMEOUT):
        from requests.packages.urllib3.util.url import parse_url
        self._parsed_url = parse_url(url)
        self._url = url
        self.host = str(self._parsed_url.host)
        if self._parsed_url.port:
            self.port = int(self._parsed_url.port)
        else:
            self.port = 12201
        if isinstance(timeout, six.string_types):
            timeout = int(timeout)
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.host, int(self.port)))
        except IOError, e:
            raise RuntimeError('Could not connect via TCP: %s' % e)
        
    def send(self, data, headers):
        try:
            totalsent = 0
            while totalsent < len(data):
                sent = self.sock.send(data[totalsent:])
                if sent == 0:
                    raise IOError("socket connection broken")
                totalsent = totalsent + sent
        except IOError:
            try:
                self.sock.close()
                self.__init__(self._url)
                self.send(data, headers)
            except IOError:
                raise RuntimeError('Could not connect via TCP: %s' % e)
        return True

    def close(self):
        self.sock.close()

        
class ThreadedTCPTransport(AsyncTransport, TCPTransport):
    scheme = ['tcp', 'threaded+tcp']

    def get_worker(self):
        if not hasattr(self, '_worker') or not self._worker.is_alive():
            self._worker = AsyncWorker()
        return self._worker

    def send_sync(self, data, headers, success_cb, failure_cb):
        try:
            super(ThreadedTCPTransport, self).send(data, headers)
        except Exception as e:
            failure_cb(e)
        else:
            success_cb()

    def async_send(self, data, headers, success_cb, failure_cb):
        self.get_worker().queue(
            self.send_sync, data, headers, success_cb, failure_cb)

        
class UDPTransport(Transport):
    scheme = ['sync+udp']

    def __init__(self, url, timeout=DEFAULT_TIMEOUT):
        from requests.packages.urllib3.util.url import parse_url
        self._parsed_url = parse_url(url)
        self._url = url
        self.host = str(self._parsed_url.host)
        if self._parsed_url.port:
            self.port = int(self._parsed_url.port)
        else:
            self.port = 12202
        if isinstance(timeout, six.string_types):
            timeout = int(timeout)
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, data, headers):
        self.sock.sendto(data, (self.host, self.port))
        return True


class ThreadedUDPTransport(AsyncTransport, TCPTransport):
    scheme = ['udp', 'threaded+udp']

    def get_worker(self):
        if not hasattr(self, '_worker') or not self._worker.is_alive():
            self._worker = AsyncWorker()
        return self._worker

    def send_sync(self, data, headers, success_cb, failure_cb):
        try:
            super(ThreadedUDPTransport, self).send(data, headers)
        except Exception as e:
            failure_cb(e)
        else:
            success_cb()

    def async_send(self, data, headers, success_cb, failure_cb):
        self.get_worker().queue(
            self.send_sync, data, headers, success_cb, failure_cb)

        
class AsyncWorker(object):
    _terminator = object()

    def __init__(self, shutdown_timeout=DEFAULT_TIMEOUT):
        self._queue = Queue(-1)
        self._lock = threading.Lock()
        self._thread = None
        self.options = {
            'shutdown_timeout': shutdown_timeout,
        }
        self.start()

    def is_alive(self):
        return self._thread.is_alive()

    def main_thread_terminated(self):
        self._lock.acquire()
        try:
            if not self._thread:
                # thread not started or already stopped - nothing to do
                return

            # wake the processing thread up
            self._queue.put_nowait(self._terminator)

            timeout = self.options['shutdown_timeout']

            # wait briefly, initially
            initial_timeout = 0.1
            if timeout < initial_timeout:
                initial_timeout = timeout

            if not self._timed_queue_join(initial_timeout):
                # if that didn't work, wait a bit longer
                # NB that size is an approximation, because other threads may
                # add or remove items
                size = self._queue.qsize()

                print("Sentry is attempting to send %i pending error messages"
                      % size)
                print("Waiting up to %s seconds" % timeout)

                if os.name == 'nt':
                    print("Press Ctrl-Break to quit")
                else:
                    print("Press Ctrl-C to quit")

                self._timed_queue_join(timeout - initial_timeout)

            self._thread = None

        finally:
            self._lock.release()

    def _timed_queue_join(self, timeout):
        """
        implementation of Queue.join which takes a 'timeout' argument

        returns true on success, false on timeout
        """
        deadline = time.time() + timeout
        queue = self._queue

        queue.all_tasks_done.acquire()
        try:
            while queue.unfinished_tasks:
                delay = deadline - time.time()
                if delay <= 0:
                    # timed out
                    return False

                queue.all_tasks_done.wait(timeout=delay)

            return True

        finally:
            queue.all_tasks_done.release()

    def start(self):
        """
        Starts the task thread.
        """
        self._lock.acquire()
        try:
            if not self._thread:
                self._thread = threading.Thread(target=self._target)
                self._thread.setDaemon(True)
                self._thread.start()
        finally:
            self._lock.release()
            atexit.register(self.main_thread_terminated)

    def stop(self, timeout=None):
        """
        Stops the task thread. Synchronous!
        """
        self._lock.acquire()
        try:
            if self._thread:
                self._queue.put_nowait(self._terminator)
                self._thread.join(timeout=timeout)
                self._thread = None
        finally:
            self._lock.release()

    def queue(self, callback, *args, **kwargs):
        self._queue.put_nowait((callback, args, kwargs))

    def _target(self):
        while True:
            record = self._queue.get()
            try:
                if record is self._terminator:
                    break
                callback, args, kwargs = record
                try:
                    callback(*args, **kwargs)
                except Exception:
                    logger.error('Failed processing job', exc_info=True)
            finally:
                self._queue.task_done()

            time.sleep(0)
