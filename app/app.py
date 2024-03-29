import os
import json
from io import BytesIO

import pika
import requests

from s3 import s3_upload, s3_download, bucketkey
from provenance.capture import Logger, Step
from provenance.amqp import amqp_subscribe, amqp_publish

subscribe_to = os.getenv('AMQP_EXCHANGE_SUBSCRIBE','upload')
publish_to = os.getenv('AMQP_EXCHANGE_PUBLISH', 'dithered')
output_bucket = os.getenv('S3_OUTPUT_BUCKET', 'amplify-poc-output')

amqp_host = os.environ.get('AMQP_HOST', 'localhost')
amqp_user = os.getenv('RABBITMQ_USER','guest') 
amqp_pwd = os.getenv('RABBITMQ_PASSWORD','guest')
amqp_exchange = os.environ.get('AMQP_EXCHANGE_PROV', 'provenance')


logger = Logger.amqp(amqp_host, amqp_user, amqp_pwd, amqp_exchange)


def callback(msg):

    step_description = {
        'description': 'Dither an image using the Floyd-Steinberg algorithm.',
    }

    with Step(name='dither-image', description=step_description, logger=logger) as step:
        step.add_input(name='input-image', description={
            'bucket': msg['bucket'],
            'key': msg['key']
        })

        # msg from upload
        # message = dict(type="upload", bucket=bucket_name, key=file.filename)
        print(msg)        
        # fetch image path in msg from s3
        content_in = s3_download(msg['bucket'], msg['key'])
        content_in = dict(file=BytesIO(content_in))
        print('DOWNLOADED:', msg['key'])
        
        # send to containerized-image-processing
        endpoint = 'http://imageproc:8000/dither'
        content_out = bytearray()
        with requests.post(endpoint, files=content_in, stream=True) as resp:
            #TODO get status code and do error logging
            if resp.status_code == 200:
                for chunk in resp.iter_content():
                    content_out.extend(chunk)
            else:
                error_msg = resp.content.decode()
                raise ValueError(error_msg)
        print('IMG DITHERED')
        
        # upload results to s3
        output_key = f'dither/{msg["key"]}'
        output_key = os.path.splitext(output_key)[0]+'.png'
        s3_upload(output_bucket, output_key, content_out)
        print('UPLOADED:', output_key)
        
        # log provenance
        print('LOG PROVENENCE')
        
        # push to amqp
        outgoing_msg = dict(bucket=output_bucket, key=output_key)
        amqp_publish(amqp_host, amqp_user, amqp_pwd, publish_to, outgoing_msg)
        print('PUBLISHED:', outgoing_msg)

        step.add_output(name='processed-image', description=outgoing_msg)

print('HELLO WORLD')

# LISTENS TO subscribe_to AND RUNS callback for each incomming message
amqp_subscribe(amqp_host, amqp_user, amqp_pwd, subscribe_to, callback)




