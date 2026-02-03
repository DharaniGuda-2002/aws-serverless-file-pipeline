# AWS Serverless File Upload & Metadata Processing System

![AWS](https://img.shields.io/badge/AWS-Serverless-orange)
![CI/CD](https://img.shields.io/github/actions/workflow/status/DharaniGuda-2002/aws-serverless-file-pipeline/deploy.yml?label=CI%2FCD)
![Python](https://img.shields.io/badge/python-3.9-blue)
![Infrastructure](https://img.shields.io/badge/IaC-AWS%20SAM-yellow)

---

## 📌 Overview

This project is a **serverless, event-driven file upload and processing system built on AWS**.
It allows clients to securely upload files to Amazon S3 using pre-signed URLs, automatically processes uploads using AWS Lambda, and stores file metadata in DynamoDB.

The system is fully managed, scalable, and implemented using **Infrastructure as Code (AWS SAM)** with **automated CI/CD deployment via GitHub Actions**.

---

## 🧠 What This Project Does

* Generates **pre-signed S3 upload URLs** via an API
* Tracks file upload state (`UPLOADING` → `PROCESSED`)
* Automatically processes uploaded files using S3 event triggers
* Stores file metadata in DynamoDB
* Exposes an API to **query file status**

All interactions are **serverless and event-driven**.

---

## 🏗️ Architecture

```
Client
  |
  v
API Gateway
  |
  +--> GenerateUploadUrl Lambda
  |         |
  |         v
  |     DynamoDB (UPLOADING)
  |
  +--> S3 (via pre-signed URL)
            |
            v
     ProcessUploadedFile Lambda
            |
            v
        DynamoDB (PROCESSED)
```

---

## ⚙️ AWS Services Used

* **Amazon S3** – Object storage and event source
* **AWS Lambda** – Serverless compute
* **Amazon DynamoDB** – Metadata storage
* **Amazon API Gateway** – REST APIs
* **AWS CloudWatch** – Logs and debugging
* **AWS SAM** – Infrastructure as Code
* **IAM** – Permissions and security

---

## 🧩 Infrastructure (IaC)

All infrastructure is defined in `template.yaml` using **AWS Serverless Application Model (SAM)**.

Provisioned resources include:

* S3 bucket for uploads
* DynamoDB table for file metadata
* Three Lambda functions:

  * Generate upload URL
  * Process uploaded files
  * Get file status
* API Gateway endpoints
* IAM roles and policies

---

## 🔁 File Processing Flow

1. Client requests an upload URL
2. API returns a **pre-signed S3 PUT URL**
3. Client uploads file directly to S3
4. S3 triggers a Lambda function
5. Lambda extracts file metadata
6. DynamoDB record is updated to `PROCESSED`

---

## 🗄️ DynamoDB Schema

**Table: FileTable**

| Attribute     | Type   | Description                   |
| ------------- | ------ | ----------------------------- |
| `file_id`     | String | Unique file identifier (PK)   |
| `status`      | String | UPLOADING / PROCESSED         |
| `file_size`   | Number | Size of uploaded file (bytes) |
| `uploaded_at` | String | Upload timestamp (UTC)        |

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

> 🚀 The project is also **automatically deployed via GitHub Actions CI/CD** on every push to the main branch.

---

## 🧪 Testing the System

### 1. Generate upload URL

```bash
curl -X POST https://<API_ID>.execute-api.us-east-1.amazonaws.com/Prod/upload-url
```

### 2. Upload file using returned URL

```bash
curl -X PUT "<UPLOAD_URL>" --upload-file test.txt
```

### 3. Check file status

```bash
curl https://<API_ID>.execute-api.us-east-1.amazonaws.com/Prod/files/<file_id>
```

---

## 🔮 Future Enhancements

* Authentication (Amazon Cognito)
* File validation and type checks
* SQS for asynchronous processing
* Object lifecycle policies
* Large file multipart uploads
* UI frontend

---

## 📌 Summary

This project demonstrates a **production-style serverless backend** using AWS managed services, covering API design, event-driven processing, security, Infrastructure as Code, and CI/CD automation.

It reflects **real-world cloud engineering challenges**, not just a basic demo.
