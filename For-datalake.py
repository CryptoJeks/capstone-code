import json
import boto3
from botocore.exceptions import ClientError

client = boto3.client('kinesis')


class KinesisStream:
    """Encapsulates a Kinesis stream."""

    def __init__(self, kinesis_client, stream_name):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = kinesis_client
        self.name = stream_name
        self.stream_exists_waiter = kinesis_client.get_waiter("stream_exists")

    def put_record(self, data, partition_key):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the stream.

        :param data: The data to put in the stream.
        :param partition_key: The partition key to use for the data.
        :return: Metadata about the record, including its shard ID and sequence number.
        """
        try:
            response = self.kinesis_client.put_record(
                StreamName=self.name,
                Data=json.dumps(data),
                PartitionKey=partition_key
            )
            print(f"Put record in stream {self.name}.")
            return response
        except ClientError as e:
            print(f"Couldn't put record in stream {self.name}. Error: {e}")
            raise


def lambda_handler(event, context):
    # Print the incoming event for debugging
    print(f'Event: {event}')

    stream_name = 'samson_capstone1'

    # # Lists for generating combinations
    vehicle_make = ["Toyota", "Honda", "KIA"]
    year_manufacture = ["2012", "2014", "2010"]
    country_make = ["USA", "JAPAN", "INDIA"]
    milleage_km = ["100KM", "150KM", "80KM"]
    price_car = ["$10,000", "$15,000", "$12,000"]
    engine_size = ["2.0", "3.5", "2.2"]

# Lists for generating combinations
    # vehicle_make = ["Toyota"]
    # year_manufacture = ["2012"]
    # country_make = ["USA"]
    # milleage_km = ["100KM"]
    # price_car = ["$10,000"]
    # engine_size = ["2.0", "3.5", "2.2"]

    # Generating combinations of attributes
    kinesis_msgs = []
    for a in vehicle_make:
        for b in year_manufacture:
            for c in country_make:
                for d in milleage_km:
                    for e in price_car:
                        for f in engine_size:
                            kinesis_msgs.append({
                                "vehicle_make": a,
                                "year_manufacture": b,
                                "country_make": c,
                                "milleage_km": d,
                                "price_car": e,
                                "engine_size": f
                            })
    ks = KinesisStream(client, stream_name)
    # You might want to send only one message or handle all messages
    for kinesis_msg in kinesis_msgs:
        try:
            result = ks.put_record(kinesis_msg, 'SinglePartition')
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error putting record: {str(e)}')
            }

    return {
        'statusCode': 200,
        'body': json.dumps('Records sent to Kinesis!'),
        'count': len(kinesis_msgs)
    }
