
from cloudwatch import Cloudwatch
from edx.analytics.monitor.monitor import ElasticMapreduceMonitor, DiskUsageMonitor
from state import State


def main():
    state = State()
    metric_collector = Cloudwatch()
    monitors = [
        ElasticMapreduceMonitor(),
        DiskUsageMonitor(),
    ]

    for monitor in monitors:
        metrics = monitor.update(state)

        metric_collector.send(metrics)
        state.update(metrics)

    state.save()


if __name__ == '__main__':
    main()
