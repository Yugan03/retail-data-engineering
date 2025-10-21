# ===============================================================
# Load Step - Retail Analytics Pipeline
# Author: Yugandhar Ailuri
# Description: Uploads the cleaned retail sales data to an AWS S3 bucket.
# ===============================================================

import os
import boto3

BUCKET_NAME = "yugan-retail-pipeline"
LOCAL_FILE_PATH = r"c:/Users/YUGANDHAR REDDY/OneDrive/Desktop/retail_analytics_pipeline/data/processed/retail_clean.csv"
S3_OBJECT_NAME = "processed/retail_clean.csv"


def upload_to_s3():
    """ uploads the cleaned data to an S3 bucket """
    s3 = boto3.client("s3")

    if not os.path.exists(LOCAL_FILE_PATH):
        print("Local file not found!")
        return

    try:
        print(
            f"Uploading{LOCAL_FILE_PATH} to S3 bucket: //{BUCKET_NAME}/{S3_OBJECT_NAME}")
        s3.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, S3_OBJECT_NAME)
        print("File successfully uploaded to s3")
    except Exception as e:
        print("Upload failed:", e)


if __name__ == "__main__":
    upload_to_s3()
