# AWS Serverless File Upload & Metadata Processing System

## 📌 Overview

This project implements a **serverless, event-driven backend system on AWS** that automatically processes files uploaded to cloud storage and stores metadata about those files in a database.

The system is fully managed, scalable, and does not require provisioning or maintaining servers. It demonstrates real-world backend patterns used for **file ingestion pipelines, document processing systems, and cloud-native architectures**.

---

## 🧠 What This Project Does

* Files are uploaded to an Amazon S3 bucket
* An S3 upload event automatically triggers an AWS Lambda function
* The Lambda function processes the upload event
* File metadata is stored in Amazon DynamoDB

This entire flow is **event-driven** and **serverless**.

---

## 🏗️ Architecture

```
       +-------------+
       | User / CLI  |
       +-------------+
              |
              v
       +-------------+
       |  S3 Bucket  |
       +-------------+
              |
              v
       +------------------+
       | Lambda Function  |
       +------------------+
              |
              v
       +----------------+
       |  DynamoDB Table |
       +----------------+
```

---

## ⚙️ AWS Services Used

* **Amazon S3** – Object storage and event source
* **AWS Lambda** – Serverless compute for processing uploads
* **Amazon DynamoDB** – NoSQL database for metadata storage
* **AWS CloudWatch** – Logging and monitoring
* **AWS SAM** – Infrastructure as Code and deployment
* **IAM** – Permissions and security

---

## 🧩 Infrastructure (IaC)

All infrastructure is defined using **AWS Serverless Application Model (SAM)** in `template.yaml`.

Resources provisioned:

* S3 bucket for file uploads
* Lambda function triggered by S3 events
* DynamoDB table for file metadata
* IAM roles and permissions

---

## 🔁 File Processing Flow

1. A file is uploaded to the S3 bucket
2. S3 emits an `ObjectCreated` event
3. Lambda is triggered automatically
4. Lambda extracts metadata from the event:

   * File name
   * File size
   * Upload timestamp
5. Metadata is written to DynamoDB with status `PROCESSED`

---

## 🗄️ DynamoDB Schema

**Table: FileTable**

| Attribute     | Type   | Description               |
| ------------- | ------ | ------------------------- |
| `file_id`     | String | File name (Primary Key)   |
| `status`      | String | Processing status         |
| `file_size`   | Number | Size of the uploaded file |
| `uploaded_at` | String | Upload timestamp (UTC)    |

---

## 🚀 Deployment

### Prerequisites

* AWS account
* AWS CLI configured
* AWS SAM CLI installed
* Python 3.9+

### Build

```bash
sam build
```

### Deploy

```bash
sam deploy --guided
```

---

## 🧪 Testing the System

Upload a test file to the S3 bucket:

```bash
echo "test upload" > test.txt
aws s3 cp test.txt s3://<BUCKET_NAME>/test.txt
```

Verify metadata in DynamoDB:

```bash
aws dynamodb scan \
  --table-name <DYNAMODB_TABLE_NAME> \
  --region us-east-1
```

---

## 🔮 Future Enhancements

* API Gateway for upload/status APIs
* Pre-signed S3 upload URLs
* File type validation
* Authentication (Amazon Cognito)
* Asynchronous processing (SQS)
* Enhanced metadata extraction

---

## 📌 Summary

This project demonstrates how to build a **scalable, serverless file processing pipeline** using AWS managed services, following best practices for security, deployment, and architecture.

It is designed to reflect **real-world backend systems**, not toy examples.
