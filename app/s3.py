import os
import io

import boto3


def bucketkey(url:str):
    assert url.startswith('s3://'), f'URL "{url}" must start with "s3://"'
    return url.replace('s3://','').split('/',1)


def s3_upload(bucket, key, file_contents):
    # Configure S3 client using environment variables
    s3_client = boto3.client( 's3',
        endpoint_url=os.environ['S3_URL'],
        aws_access_key_id=os.environ['S3_ACCESS_KEY'],
        aws_secret_access_key=os.environ['S3_SECRET_KEY']
    )

    # Upload the file to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=file_contents
    )
    
def s3_down_(bucket, key):
    # Configure S3 client using environment variables
    s3_client = boto3.client( 's3',
        endpoint_url=os.environ['S3_URL'],
        aws_access_key_id=os.environ['S3_ACCESS_KEY'],
        aws_secret_access_key=os.environ['S3_SECRET_KEY']
    )

    # Fetch file from S3
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    return resp
    
    
def s3_download(bucket, key, write_to=None):
    resp = s3_down_(bucket, key)
    content = resp['Body'].read()
    if write_to:
        with open(write_to, 'wb') as f:
            f.write(content)
    return content


def s3_downstream(bucket, key): 
    resp = s3_down_(bucket, key)
    return resp['Body']
    
    
