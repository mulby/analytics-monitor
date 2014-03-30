
import re
import requests


class HadoopJmx(object):

    def __init__(self, host='localhost', port=9100):
        self.endpoint = 'http://{host}:{port}/jmx'.format(host=host, port=port)

    @property
    def heap_used_percent(self):
        response = requests.get(self.endpoint)
        parsed_response = response.json()
        for bean in parsed_response.get('beans'):
            name = bean.get('name', '')
            if name == 'java.lang:type=Memory':
                heap_stats = bean.get('HeapMemoryUsage')
                if heap_stats is not None:
                    committed = heap_stats.get('committed')
                    maximum = heap_stats.get('max')
                    return (float(committed) / maximum) * 100
