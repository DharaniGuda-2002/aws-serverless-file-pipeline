import os
import boto3
from datetime import datetime

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["TABLE_NAME"]
table = dynamodb.Table(TABLE_NAME)

def handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    response = s3.head_object(Bucket=bucket, Key=key)

    table.update_item(
        Key={"file_id": key},
        UpdateExpression="""
            SET #status = :status,
                file_size = :size,
                file_type = :type,
                uploaded_at = :uploaded_at
        """,
        ExpressionAttributeNames={
            "#status": "status"
        },
        ExpressionAttributeValues={
            ":status": "PROCESSED",
            ":size": response["ContentLength"],
            ":type": response.get("ContentType", "unknown"),
            ":uploaded_at": datetime.utcnow().isoformat() + "Z"
        }
    )
