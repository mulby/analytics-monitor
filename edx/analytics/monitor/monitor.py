from collections import namedtuple
import subprocess
import re

from hadoopjmx import HadoopJmx


Metric = namedtuple('Metric', ['name', 'value', 'unit'])


class JobTrackerMonitor(object):

    def __init__(self):
        self.jmx = HadoopJmx()

    def update(self):
        return [
            Metric('JobTrackerHeapUsage', self.jmx.heap_used_percent, 'Percent')
        ]


class NameNodeMonitor(object):

    def __init__(self):
        self.jmx = HadoopJmx(port=9101)

    def update(self):
        return [
            Metric('NameNodeHeapUsage', self.jmx.heap_used_percent, 'Percent')
        ]


class DiskUsageMonitor(object):

    def update(self):
        proc = subprocess.Popen(['df'], stdout=subprocess.PIPE)
        (stdout, _stderr) = proc.communicate()
        if proc.returncode != 0:
            return

        metrics = []
        for line in stdout.strip().splitlines():
            if line.startswith('Filesystem'):
                continue

            columns = re.split('\s+', line)
            device_name = columns[0]
            used = int(columns[2])
            available = int(columns[3])

            if not device_name.startswith('/dev'):
                continue

            name = device_name.split('/')[-1].title()
            percent_used = (float(used) / (used + available)) * 100

            metrics.append(Metric('DiskUsage' + name, percent_used, 'Percent'))

        return metrics

