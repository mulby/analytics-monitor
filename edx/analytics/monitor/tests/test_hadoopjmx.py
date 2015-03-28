
import json
import os
import sys
if sys.version_info[:2] <= (2, 6):
    import unittest2 as unittest
else:
    import unittest

from mock import patch

from edx.analytics.monitor import hadoopjmx


class TestHadoopJmx(unittest.TestCase):

    @patch('edx.analytics.monitor.hadoopjmx.requests')
    def test_jobtracker_heap(self, mock_requests):
        mock_requests.get.return_value = FakeResponse('jobtracker_jmx.json')

        jmx = hadoopjmx.HadoopJmx()
        self.assertAlmostEqual(jmx.heap_used_percent, 6.982421875)
        self.assertEquals(jmx.gc_mark_sweep_count, 807)
        self.assertEquals(jmx.gc_mark_sweep_time, 3069165)
        self.assertEquals(jmx.gc_copy_count, 398)
        self.assertEquals(jmx.gc_copy_time, 10108)

        mock_requests.get.assert_called_once_with('http://localhost:9100/jmx')


class FakeResponse(object):

    def __init__(self, filename):
        self.filename = filename

    def json(self):
        path = os.path.join(os.path.dirname(__file__), 'data', self.filename)
        with open(path, 'r') as data_file:
            return json.load(data_file)
