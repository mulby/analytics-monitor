
from cloudwatch import Cloudwatch
from edx.analytics.monitor.monitor import JobTrackerMonitor, NameNodeMonitor, DiskUsageMonitor


def main():
    metric_collector = Cloudwatch()
    monitors = [
        JobTrackerMonitor(),
        NameNodeMonitor(),
        DiskUsageMonitor(),
    ]

    for monitor in monitors:
        metric_collector.send(monitor.update())


if __name__ == '__main__':
    main()
