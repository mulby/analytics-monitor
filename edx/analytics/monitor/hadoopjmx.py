
import re
import requests


#"name" : "Hadoop:service=JobTracker,name=jvm"

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
            elif re.match(r'Hadoop:service=\w+,name=jvm', name):
                self.gc_count = bean.get('gcCount', -1)
                self.gc_time = bean.get('gcTimeMillis', 0)
