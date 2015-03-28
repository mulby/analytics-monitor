
import requests


class HadoopJmx(object):

    def __init__(self, host='localhost', port=9100):
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
                count = bean.get('CollectionCount')
                if count:
                    self.gc_mark_sweep_count = count
                time = bean.get('CollectionTime')
                if time:
                    self.gc_mark_sweep_time = time
            elif name == 'java.lang:type=GarbageCollector,name=Copy':
                count = bean.get('CollectionCount')
                if count:
                    self.gc_copy_count = count
                time = bean.get('CollectionTime')
                if time:
                    self.gc_copy_time = time
