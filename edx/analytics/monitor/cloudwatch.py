
import boto
import requests


class Cloudwatch(object):

    NAMESPACE = 'Analytics/Monitor'

    def __init__(self):
        self.cloudwatch = boto.connect_cloudwatch()
        self.instance_id = Cloudwatch.get_instance_id()

    @staticmethod
    def get_instance_id():
        response = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
        return response.text.strip()

    def send(self, metrics, dimensions=None):
        for metric in metrics:
            if metric.value is None:
                return

            dimensions_param = {
                "InstanceId": self.instance_id
            }
            if dimensions:
                dimensions_param.update(dimensions)

            self.cloudwatch.put_metric_data(
                namespace=self.NAMESPACE,
                name=metric.name,
                value=metric.value,
                unit=metric.unit,
                dimensions=dimensions_param,
            )
