import sys
from textwrap import dedent
if sys.version_info[:2] <= (2, 6):
    import unittest2 as unittest
else:
    import unittest

from mock import patch

from edx.analytics.monitor.monitor import DiskUsageMonitor, Metric


class TestDiskUsageMonitor(unittest.TestCase):

    @patch('edx.analytics.monitor.monitor.subprocess.Popen')
    def test_update(self, MockPopen):
        mock_proc = MockPopen.return_value
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = (dedent("""
            Filesystem           1K-blocks      Used Available Use% Mounted on
            /dev/xvda1            10321208   3045024   6751904  32% /
            tmpfs                  3816936         0   3816936   0% /lib/init/rw
            udev                   3806104        52   3806052   1% /dev
            tmpfs                  3816936         4   3816932   1% /dev/shm
            /dev/xvdb            440151060  11498776 428652284   3% /mnt
            /dev/xvdc            440151060     38040 440113020   1% /mnt1
            """
        ), None)

        monitor = DiskUsageMonitor()
        metrics = monitor.update()

        expected_metrics = [
            Metric('DiskUsageXvda1', 31.081416542, 'Percent'),
            Metric('DiskUsageXvdb', 2.612461276, 'Percent'),
            Metric('DiskUsageXvdc', 0.008642487, 'Percent'),
        ]
        for actual, expected in zip(metrics, expected_metrics):
            self.assertEquals(actual.name, expected.name)
            self.assertAlmostEqual(actual.value, expected.value)
            self.assertEquals(actual.unit, expected.unit)
