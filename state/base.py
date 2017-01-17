import abc
import state


class RPCStateBaseRepository(object):

    @abc.abstractmethod
    def on_incoming(self, state_sample):
        pass

    @abc.abstractmethod
    def hosts(self):
        pass

    @abc.abstractmethod
    def services(self):
        pass

    @abc.abstractmethod
    def workers(self):
        pass

    def register(self):
        state.register_state_repository(self)


class loop_bucket(object):
    """
       container for time loop metrics:
        min, max, sum, count
    """

    MIN = 0
    MAX = 1
    SUM = 2
    CNT = 3

    @classmethod
    def set(cls, bucket, value):
        bucket[cls.SUM] = value
        bucket[cls.CNT] = 1
        bucket[cls.MAX] = bucket[cls.MIN] = value

    @classmethod
    def add(cls, bucket, value):
        bucket[cls.SUM] += value
        bucket[cls.CNT] += 1
        bucket[cls.MIN] = min(bucket[cls.MIN], value)
        bucket[cls.MAX] = max(bucket[cls.MAX], value)

    @classmethod
    def create(cls):
        return [0, 0, 0, 0]

    @classmethod
    def get(cls, item, bucket):
        return bucket[item] if bucket else 0

    @classmethod
    def get_max(cls, bucket):
        return cls.get(cls.MAX, bucket)

    @classmethod
    def get_min(cls, bucket):
        return cls.get(cls.MIN, bucket)

    @classmethod
    def get_sum(cls, bucket):
        return cls.get(cls.SUM, bucket)

    @classmethod
    def get_cnt(cls, bucket):
        return cls.get(cls.CNT, bucket)

    @classmethod
    def get_avg(cls, bucket):
        return cls.get_sum(bucket) / (cls.get_cnt(bucket) or 1)
