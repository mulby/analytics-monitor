import json
from collections import namedtuple
import subprocess
import re

from hadoopjmx import HadoopJmx


Metric = namedtuple('Metric', ['name', 'value', 'unit'])


# cat /mnt/var/lib/info/instance.json 
# {
#   "instanceGroupId": "ig-3KYROERBW168Z",
#   "isMaster": true,
#   "isRunningNameNode": true,
#   "isRunningDataNode": false,
#   "isRunningJobTracker": true,
#   "isRunningTaskTracker": false,
#   "isRunningResourceManager": false,
#   "isRunningNodeManager": false
# }


class ElasticMapreduceMonitor(object):

    def update(self):
        with open('/mnt/var/lib/info/instance.json', 'r') as info_file:
            instance_info = json.load(info_file)

        metrics = []
        if instance_info['isRunningJobTracker']:
            metrics += JobTrackerMonitor().update()
        if instance_info['isRunningNameNode']:
            metrics += NameNodeMonitor().update()
        if instance_info['isRunningTaskTracker']:
            metrics += TaskTrackerMonitor().update()

        return metrics


class JvmMonitor(object):

    def __init__(self, name, port=None):
        self.name = name
        self.jmx = HadoopJmx(port=port)

    def update(self, state):
        prev_gc_count = state.get_previous('{0}GcCount'.format(self.name))
        if prev_gc_count:
            gc_rate = (self.jmx.gc_count - prev_gc_count) / state.elapsed_seconds
        else:
            gc_rate = 0

        prev_gc_time = state.get_previous('{0}GcTime'.format(self.name))
        if prev_gc_time:
            gc_time_ratio = ((self.jmx.gc_time - prev_gc_time) * 1000) / state.elapsed_seconds
        else:
            gc_time_ratio = 0

        return [
            Metric('{0}HeapUsage'.format(self.name), self.jmx.heap_used_percent, 'Percent'),
            Metric('{0}GcCount'.format(self.name), self.jmx.gc_count, 'None'),
            Metric('{0}GcTime'.format(self.name), self.jmx.gc_time, 'Milliseconds'),
            Metric('{0}GcCountRate'.format(self.name), gc_rate, 'Count/Second'),
            Metric('{0}GcTimeRatio'.format(self.name), gc_time_ratio * 100, 'Percent'),
        ]


class JobTrackerMonitor(JvmMonitor):

    def __init__(self):
        super(JobTrackerMonitor, self).__init__('JobTracker', port=9100)


class NameNodeMonitor(JvmMonitor):

    def __init__(self):
        super(NameNodeMonitor, self).__init__('NameNode', port=9101)


class TaskTrackerMonitor(JvmMonitor):

    def __init__(self):
        super(NameNodeMonitor, self).__init__('TaskTracker', port=9103)


class DiskUsageMonitor(object):

    def update(self, state):
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
