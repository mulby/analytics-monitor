
import datetime
import json


STATE_FILE_PATH = '/var/tmp/analytics-monitor.json'
TIMESTAMP_KEY = '_timestamp'
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class State(object):

    def __init__(self):
        self.timestamp = datetime.datetime.utcnow()
        self.metrics = {
            TIMESTAMP_KEY: self.timestamp.strftime(TIMESTAMP_FORMAT)
        }
        self.previous = PreviousState()
        if self.previous.is_valid:
            self.elapsed_seconds = float((self.previous.timestamp - self.timestamp).total_seconds())
        else:
            self.elapsed_seconds = 0.00000000001

    def update(self, metrics):
        for metric in metrics:
            self.metrics[metric.name] = metric.value

    def save(self):
        with open(STATE_FILE_PATH, 'w') as state_file:
            json.dump(self.metrics, state_file)

    def get_previous(self, metric_name):
        return self.previous.metrics.get(metric_name)


class PreviousState(object):

    def __init__(self):
        try:
            with open(STATE_FILE_PATH, 'r') as state_file:
                self.metrics = json.load(state_file)
                self.timestamp = datetime.datetime.strptime(self.metrics[TIMESTAMP_KEY], TIMESTAMP_FORMAT)
                self.is_valid = True
        except IOError:
            self.metrics = {}
            self.is_valid = False

    def get(self, metric_name):
        return self.metrics.get(metric_name)
