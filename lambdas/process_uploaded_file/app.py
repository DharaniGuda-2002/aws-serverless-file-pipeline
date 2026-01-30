import boto3
import os
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

def handler(event, context):
    record = event["Records"][0]
    key = record["s3"]["object"]["key"]
    size = record["s3"]["object"]["size"]

    table.put_item(
        Item={
            "file_id": key,
            "status": "PROCESSED",
            "file_size": size,
            "uploaded_at": datetime.utcnow().isoformat() + "Z"
        }
    )
