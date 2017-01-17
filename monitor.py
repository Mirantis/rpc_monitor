import Queue
import logging

import time
from threading import Thread

from concurrent.futures import ThreadPoolExecutor
from oslo_config import cfg
from client.rabbit_client import KombuStateClient, PikaStateClient
from state.influx_repository import InfluxDBStateRepository

LOG = logging.Logger(__name__)


class RPCStateMonitor(object):
    def __init__(self):
        config = self.setup_config()
        # repository for storing and processing incoming state samples
        self.repository = InfluxDBStateRepository(config.poll_delay)
        self.callbacks_routes = {'sample': self.repository.on_incoming}
        # setup workers for processing incoming states
        self.incoming = Queue.Queue()
        self.client = self.build_client(config)
        self.workers = ThreadPoolExecutor(config.workers)

        for _ in xrange(config.workers):
            self.workers.submit(self.worker)

        self.poll_delay = config.poll_delay
        if not config.listener_only:
            self.periodic_updates = Thread(target=self.update_rpc_state)
            self.periodic_updates.start()

    def worker(self):
        while True:
            resp_time, response = self.incoming.get()
            msg_type = response['msg_type']
            self.callbacks_routes[msg_type](resp_time, response)

    def setup_config(self):
        opts = [cfg.IntOpt(name='poll_delay', default=60),
                cfg.IntOpt(name='workers', default=8),
                cfg.BoolOpt(name='listener_only', default=False),
                cfg.StrOpt(name="rpc_backend", default='kombu',
                           choices=['kombu', 'pika', 'zmq'])]
        config = cfg.CONF
        config.register_opts(opts)
        return config

    def build_client(self, cfg):
        if cfg.rpc_backend == 'kombu':
            return KombuStateClient(self.on_incoming)
        elif cfg.rpc_backend == 'pika':
            return PikaStateClient(self.on_incoming)
        elif cfg.rpc_backend == 'zmq':
            raise NotImplemented()

    def on_incoming(self, response):
        self.incoming.put((time.time(), response))

    def update_exchanges_list(self):
        pass
        # exchanges = self.client.rabbit_client.exchanges_list()
        # self.client.setup_exchange_bindings(exchanges)

    def update_rpc_state(self):
        while True:
            request_time = time.time()
            self.client.get_rpc_stats(request_time)
            time.sleep(self.poll_delay)
            self.update_exchanges_list()
