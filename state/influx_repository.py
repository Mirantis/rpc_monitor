from Queue import Queue, Empty
from threading import Thread

import time
import logging
from influxdb import InfluxDBClient
from oslo_config import cfg
from concurrent.futures import ThreadPoolExecutor
from state.base import loop_bucket

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
LOG.addHandler(logging.StreamHandler())

class InfluxDBReporter(object):
    def __init__(self):
        self.conf = self.setup_conf()
        self.samples_queue = Queue(maxsize=5000)
        self.influx_client = InfluxDBClient(self.conf.host,
                                            self.conf.port,
                                            self.conf.user,
                                            self.conf.password,
                                            self.conf.db)

        self.influx_client.create_database(self.conf.db)
        self.pool = ThreadPoolExecutor(4)
        for _ in range(4):
            self.pool.submit(self._sample_sender)

    @staticmethod
    def setup_conf():

        opt_group = cfg.OptGroup(name='influx_repository')
        opts = [cfg.StrOpt('host', default='localhost'),
                cfg.IntOpt('port', default=8086),
                cfg.StrOpt('user', default='root'),
                cfg.StrOpt('password', default='root'),
                cfg.StrOpt('db', default='rpc_monitor')]

        config = cfg.CONF
        config.register_group(opt_group)
        config.register_opts(opts, group=opt_group)

        return config.influx_repository

    def _populate_batch(self, batch, max_size=1000):
        try:
            timestamp = time.time()
            while 1:
                batch.append(self.samples_queue.get(timeout=5))
                max_size -= 1
                if max_size == 0 or time.time() - timestamp > 5:
                    break
        except Empty:
            pass
        LOG.info("[InfluxRepository] Sending batch of states %s " % len(batch))

    def _sample_sender(self):
        batch = []
        while 1:
            batch.append(self.samples_queue.get())
            self._populate_batch(batch)
            self.influx_client.write_points(batch)
            batch = []


class StateWatcher(object):
    def __init__(self, influx_client, samples_queue, update_time):
        self.is_running = False
        self.samples_queue = samples_queue
        self.influx_client = influx_client
        self.new_workers, self.die_workers = self._workers_events()
        self.worker_thread = Thread(target=self.worker)
        self.worker_timeout = max(update_time, 120)  # (s)

    def worker(self):
        while True:
            self.report_workers_state()
            time.sleep(self.worker_timeout)

    def _as_id(self, tags):
        return '|'.join([tags['process_name'], tags['host'], tags['wid']])

    def _workers_events(self):

        events_q = 'select * from workers_state where time > now() - 1h'
        result = self.influx_client.query(events_q).get_points(measurement='workers_state')
        new, die = set(), set()
        for sample in list(result):
            if sample['value'] == 'die':
                die.add(self._as_id(sample))
            else:
                new.add(self._as_id(sample))
        return new, die

    def report_workers_state(self):

        # needs to detect new workers
        q = 'select count(latency) from response_time WHERE %s GROUP BY wid, host, process_name'
        # needs to detect die workers
        l = 'select last(response),wid,host,process_name from response_time Where %s GROUP BY wid, host, process_name'

        def detect_die():
            _latest = self.influx_client.query(l % 'time > now() - 1h')
            die = set()
            now = time.time()
            for sample in _latest.get_points(measurement='response_time'):
                if now - sample['last'] > self.worker_timeout:
                    _id = self._as_id(sample)
                    if _id not in self.die_workers:
                        die.add(_id)
            return die

        def detect_new():
            _ten = self.influx_client.query(q % 'time > now() - 10m')
            _hour = self.influx_client.query(q % 'now() - 1h < time < now() - 10m')
            actual_workers = set(self._as_id(x[1]) for x in _ten.keys())
            history_workers = set(self._as_id(x[1]) for x in _hour.keys())
            new = (actual_workers - history_workers) - self.new_workers
            return new

        def report_workers(workers, state):
            for worker in workers:
                proc, host, wid = worker.split('|')
                self.samples_queue.put({
                    'measurement': 'workers_state',
                    'time': int(time.time() * 10 ** 9),
                    'tags': {
                        'host': host,
                        'process_name': proc,
                        'wid': wid
                    }, 'fields': {
                        'value': state
                    }
                })

        die_workers = detect_die()
        new_workers = detect_new()

        self.new_workers |= new_workers
        self.die_workers |= die_workers

        report_workers(new_workers, 'up')
        report_workers(die_workers, 'down')

    def start(self):
        self.worker_thread.start()

    def stop(self):
        # unstoppable ;)
        pass


class InfluxDBStateRepository(InfluxDBReporter):
    def __init__(self, update_time):
        super(InfluxDBStateRepository, self).__init__()
        self.state_watcher = StateWatcher(self.influx_client, self.samples_queue, update_time)
        self.state_watcher.start()
        self.cache = {}

    @staticmethod
    def over_methods(sample):
        for endpoint, methods in sample['endpoints'].iteritems():
            for method, state in methods.iteritems():
                yield endpoint, method, state

    def report_response_time(self, resp_time, msg):
        request_time = msg['req_time']
        self.samples_queue.put({
            'measurement': 'response_time',
            'time': int(resp_time * 10 ** 9),
            'tags': {
                'host': msg['hostname'],
                'wid': msg['wid'],
                'process_name': msg['proc_name'],
            },
            'fields': {
                'latency': (resp_time - request_time),
                'response': resp_time
            }
        })

    def service_id(self, s):
        return '|'.join([s['topic'], s['server'], s['hostname'], str(s['wid']), s['proc_name']])

    def report_rpc_stats(self, msg):
        service_id = self.service_id(msg)
        for endpoint, method, state in self.over_methods(msg):
            method_id = service_id + '.%s.%s' % (endpoint, method)

            if method_id not in self.cache:
                self.cache[method_id] = 0

            if self.cache[method_id] != state['latest_call']:
                self.cache[method_id] = state['latest_call']
            else:
                continue

            aligned_time = int(state['latest_call'] - state['latest_call'] % msg['granularity'])
            for bucket in reversed(state['distribution']):
                if loop_bucket.get_cnt(bucket):
                    method_sample = {
                        'measurement': 'rpc_method',
                        'time': aligned_time * 1000000000,
                        'tags': {
                            'topic': msg['topic'],
                            'server': msg['server'],
                            'host': msg['hostname'],
                            'wid': msg['wid'],
                            'process_name': msg['proc_name'],
                            'endpoint': endpoint,
                            'method': method
                        },
                        'fields': {
                            'avg': float(loop_bucket.get_avg(bucket)),
                            'sum': loop_bucket.get_cnt(bucket) * float(loop_bucket.get_avg(bucket)),
                            'max': float(loop_bucket.get_max(bucket)),
                            'min': float(loop_bucket.get_min(bucket)),
                            'cnt': loop_bucket.get_cnt(bucket)
                        }
                    }
                    self.samples_queue.put(method_sample)
                aligned_time -= msg['granularity']

    def on_incoming(self, resp_time, state_sample):
        self.report_response_time(resp_time, state_sample)
        self.report_rpc_stats(state_sample)
        self.report_processing_delay(state_sample)
        self.report_reply_delay(state_sample)

    def report_processing_delay(self, msg):
        delay_state = msg['proc_delay']
        aligned_time = int(delay_state['latest_call'] - delay_state['latest_call'] % msg['granularity'])
        for bucket in reversed(delay_state['distribution']):
            if loop_bucket.get_cnt(bucket):
                method_sample = {
                    'measurement': 'processing_delay',
                    'time': aligned_time * 1000000000,
                    'tags': {
                        'topic': msg['topic'],
                        'server': msg['server'],
                        'host': msg['hostname'],
                        'wid': msg['wid'],
                        'process_name': msg['proc_name']
                    },
                    'fields': {
                        'avg': float(loop_bucket.get_avg(bucket)),
                        'sum': loop_bucket.get_cnt(bucket) * float(loop_bucket.get_avg(bucket)),
                        'max': float(loop_bucket.get_max(bucket)),
                        'min': float(loop_bucket.get_min(bucket)),
                        'cnt': loop_bucket.get_cnt(bucket)
                    }
                }
                self.samples_queue.put(method_sample)
            aligned_time -= msg['granularity']

    def report_reply_delay(self, msg):
        delay_state = msg['reply_delay']
        aligned_time = int(delay_state['latest_call'] - delay_state['latest_call'] % msg['granularity'])
        for bucket in reversed(delay_state['distribution']):
            if loop_bucket.get_cnt(bucket):
                method_sample = {
                    'measurement': 'reply_delay',
                    'time': aligned_time * 1000000000,
                    'tags': {
                        'topic': msg['topic'],
                        'server': msg['server'],
                        'host': msg['hostname'],
                        'wid': msg['wid'],
                        'process_name': msg['proc_name']
                    },
                    'fields': {
                        'avg': float(loop_bucket.get_avg(bucket)),
                        'sum': loop_bucket.get_cnt(bucket) * float(loop_bucket.get_avg(bucket)),
                        'max': float(loop_bucket.get_max(bucket)),
                        'min': float(loop_bucket.get_min(bucket)),
                        'cnt': loop_bucket.get_cnt(bucket)
                    }
                }
                self.samples_queue.put(method_sample)
            aligned_time -= msg['granularity']
