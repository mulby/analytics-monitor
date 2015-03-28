
import requests


class HadoopJmx(object):

    def __init__(self, host='localhost', port=None):
        if not port:
            port = 9100

        self.endpoint = 'http://{host}:{port}/jmx'.format(host=host, port=port)

        response = requests.get(self.endpoint)
        parsed_response = response.json()
        for bean in parsed_response.get('beans'):
            name = bean.get('name', '')
            if name == 'java.lang:type=Memory':
                heap_stats = bean.get('HeapMemoryUsage')
                if heap_stats is not None:
                    committed = heap_stats.get('committed')
                    maximum = heap_stats.get('max')
                    self.heap_used_percent = (float(committed) / maximum) * 100
            elif name == 'java.lang:type=GarbageCollector,name=MarkSweepCompact':
                self.extract_gc_stats(bean, 'mark_sweep')
            elif name == 'java.lang:type=GarbageCollector,name=Copy':
                self.extract_gc_stats(bean, 'copy')

    def extract_gc_stats(self, bean, prefix):
        count = bean.get('CollectionCount')
        if count:
            setattr(self, 'gc_{0}_count'.format(prefix), count)

        time = bean.get('CollectionTime')
        if time:
            setattr(self, 'gc_{0}_time'.format(prefix), time)

    @property
    def gc_count(self):
        return self.gc_mark_sweep_count + self.gc_copy_count

    @property
    def gc_time(self):
        return self.gc_mark_sweep_time + self.gc_copy_time
