import json
import boto3


def lambda_handler(event, context):
    # TODO implement
    # lambda is triggered by SNS which has message from S3
    # get the bucket name and object key from S3 message
    message = event['Records'][0]['Sns']['Message']

    # get bucket name and object key from message
    message = json.loads(message)
    bucket_name = message['Records'][0]['s3']['bucket']['name']
    objectkey = message['Records'][0]['s3']['object']['key']

    # read s3 object
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=objectkey)
    body = obj['Body'].read().decode('utf-8')

    # read each line and convert to json before putting this in dyanmodb
    for line in body.splitlines():
        json_line = json.loads(line)

        dynamodb = boto3.resource('dynamodb')

        table = dynamodb.Table('vehicleinfo')
        table.put_item(Item=json_line)

    return {
        'statusCode': 200,
        'body': json.dumps('Records inserted in dynamodb')
    }
