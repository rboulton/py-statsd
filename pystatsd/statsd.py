# statsd.py

# Steve Ivy <steveivy@gmail.com>
# http://monkinetic.com

import contextlib
import logging
import socket
import random
import time


class BaseClient(object):

    def __init__(self, host='127.0.0.1', port=8125, prefix=None):
        self.host = host
        self.port = int(port)
        self.prefix = prefix
        self.log = logging.getLogger("pystatsd.client")

    @contextlib.contextmanager
    def timer(self, stats, sample_rate=1):
        start = time.time()
        yield
        self.timing_since(stats, start, sample_rate)

    def timing_since(self, stats, start, sample_rate=1):
        """
        Log timing information as the number of milliseconds since the provided time float
        >>> start = time.time()
        >>> # do stuff
        >>> statsd_client.timing_since('some.time', start)
        """
        self.timing(stats, (time.time() - start) * 1000, sample_rate)

    def timing(self, stats, time, sample_rate=1):
        """
        Log timing information for one or more stats, in milliseconds
        >>> statsd_client.timing('some.time', 500)
        """
        if not isinstance(stats, list):
            stats = [stats]
        data = dict((stat, "%f|ms" % time) for stat in stats)
        self.send(data, sample_rate)

    def gauge(self, stat, value, sample_rate=1):
        """
        Log gauge information for a single stat
        >>> statsd_client.gauge('some.gauge',42)
        """
        stats = {stat: "%f|g" % value}
        self.send(stats, sample_rate)

    def increment(self, stats, sample_rate=1):
        """
        Increments one or more stats counters
        >>> statsd_client.increment('some.int')
        >>> statsd_client.increment('some.int',0.5)
        """
        self.update_stats(stats, 1, sample_rate=sample_rate)

    def decrement(self, stats, sample_rate=1):
        """
        Decrements one or more stats counters
        >>> statsd_client.decrement('some.int')
        """
        self.update_stats(stats, -1, sample_rate=sample_rate)

    def update_stats(self, stats, delta, sample_rate=1):
        """
        Updates one or more stats counters by arbitrary amounts
        >>> statsd_client.update_stats('some.int',10)
        """
        if not isinstance(stats, list):
            stats = [stats]

        data = dict((stat, "%s|c" % delta) for stat in stats)
        self.send(data, sample_rate)

    def send(self, data, sample_rate=1):
        raise NotImplementedError


class Client(BaseClient):
    """Normal UDP client.  This is probably what you want to use.

    Sends statistics to the stats daemon over UDP

    """

    def __init__(self, *args, **kwargs):
        """Create a new Statsd client.

        * host: the host where statsd is listening, defaults to localhost
        * port: the port where statsd is listening, defaults to 8125
        * prefix: a prefix to prepend to all logged stats

        >>> from pystatsd import statsd
        >>> stats_client = statsd.Statsd(host, port)

        """
        super(Client, self).__init__(*args, **kwargs)
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addr = (self.host, self.port)

    def send(self, data, sample_rate=1):
        """
        Squirt the metrics over UDP
        """

        if self.prefix:
            data = dict((".".join((self.prefix, stat)), value) for stat, value in data.iteritems())

        if sample_rate < 1:
            if random.random() > sample_rate:
                return
            sampled_data = dict((stat, "%s|@%s" % (value, sample_rate)) for stat, value in data.iteritems())
        else:
            sampled_data = data

        try:
            try:
                [self.udp_sock.sendto("%s:%s" % (stat, value), self.addr) for stat, value in sampled_data.iteritems()]
            except socket.gaierror:
                self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                [self.udp_sock.sendto("%s:%s" % (stat, value), self.addr) for stat, value in sampled_data.iteritems()]
        except Exception:
            self.log.exception("unexpected error sending to %r", self.addr)


class MockClient(Client):

    def __init__(self, *args, **kwargs):
        super(Client, self).__init__(*args, **kwargs)

    def send(self, data, sample_rate=1):
        if self.prefix:
            data = dict((".".join((self.prefix, stat)), value) for stat, value in data.iteritems())

        for stat, value in data.iteritems():
            if value.endswith("|ms"):
                self.log.debug("timing(%s, %s)" % (stat, value[:-3]))
            elif value.endswith("|g"):
                self.log.debug("gauge(%s, %s)" % (stat, value[:-2]))
            elif value.endswith("|c"):
                self.log.debug("update(%s, %s)" % (stat, value[:-2]))
            else:
                self.log.debug("stat msg(%s, %s)" % (stat, value))
