import json
import uuid
import os
from datetime import datetime
import boto3

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["TABLE_NAME"]
BUCKET_NAME = os.environ["BUCKET_NAME"]

table = dynamodb.Table(TABLE_NAME)

def handler(event, context):
    file_id = str(uuid.uuid4())

    upload_url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": BUCKET_NAME,
            "Key": file_id
        },
        ExpiresIn=300
    )

    table.put_item(
        Item={
            "file_id": file_id,
            "status": "UPLOADING",
            "uploaded_at": datetime.utcnow().isoformat() + "Z"
        }
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "file_id": file_id,
            "upload_url": upload_url
        })
    }
