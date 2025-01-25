import boto3


class AWSClient:
    def __init__(self, service_name, region_name=None):
        """
        Initialize an AWS client for a specific service using default credentials.

        :param service_name: The name of the AWS service (e.g., 's3', 'ec2', 'dynamodb')
        :param region_name: The region name to use for the client (optional).
        """
        self.service_name = service_name
        self.region_name = region_name
        self.client = self.initialize_client()

    def initialize_client(self):
        """
        Create an AWS client using boto3 with the default credentials.

        :return: The boto3 client for the specified service.
        """
        if self.region_name:
            client = boto3.client(self.service_name, region_name=self.region_name)
        else:
            client = boto3.client(self.service_name)
        return client


class BatchClient(AWSClient):
    def __init__(self, region_name=None):
        """
        Initialize an AWS Batch client using default credentials.

        :param region_name: The region name to use for the client (optional).
        """
        super().__init__("batch", region_name)
