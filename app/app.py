import os
from io import BytesIO
import asyncio

import httpx

from provenance.capture import Logger, Step
from amqp.rabbit import aio_publish, aio_subscribe
from storage.s3 import AsyncBucketStore, aiobotocore

subscribe_to = os.getenv('AMQP_EXCHANGE_SUBSCRIBE','upload')
publish_to = os.getenv('AMQP_EXCHANGE_PUBLISH', 'dithered')
output_bucket = os.getenv('S3_OUTPUT_BUCKET', 'amplify-poc-output')

amqp_host = os.getenv('AMQP_HOST', 'localhost')
amqp_user = os.getenv('RABBITMQ_USER','guest') 
amqp_pwd = os.getenv('RABBITMQ_PASSWORD','guest')
amqp_exchange = os.getenv('AMQP_EXCHANGE_PROV', 'provenance')

s3_endpoint=os.getenv('S3_URL', 'http://localhost:9000')
s3_key=os.getenv('S3_ACCESS_KEY', '')
s3_pwd=os.getenv('S3_SECRET_KEY', '')

logger = Logger.amqp(amqp_host, amqp_user, amqp_pwd, amqp_exchange)


async def callback(msg):

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
        # fetch image path in msg from s3
        s3_session = aiobotocore.session.get_session()
        async with s3_session.create_client('s3', endpoint_url=s3_endpoint, 
                                         aws_secret_access_key=s3_pwd, 
                                         aws_access_key_id=s3_key) as s3_client:
            fetched_content = await AsyncBucketStore(s3_client, msg['bucket']).get(msg['key'])
            
            # send to containerized-image-processing
            endpoint = 'http://imageproc:8000/dither' # TODO use env var
            produced_content = bytearray()
            fetched_content = dict(file=BytesIO(fetched_content))  # TODO can I pass fetched_content directly to stream(..., files= _ ? See https://www.python-httpx.org/async/#streaming-requests or #streaming-responses
            async with httpx.AsyncClient().stream('POST', endpoint, files=fetched_content, 
                        follow_redirects=True, timeout=httpx.Timeout(10.0, read=None)) as resp:
                if resp.status_code == 200:
                    async for chunk in resp.aiter_bytes():
                        produced_content.extend(chunk)
                else:
                    error_msg = await resp.aread()
                    raise ValueError(error_msg)
            
            # upload results to s3
            output_key = f'dither/{msg["key"]}'
            output_key = os.path.splitext(output_key)[0]+'.png'
            await AsyncBucketStore(s3_client, output_bucket).put(output_key, produced_content)
        
        # push to amqp
        outgoing_msg = dict(bucket=output_bucket, key=output_key)
        await aio_publish(outgoing_msg, amqp_host, amqp_user, amqp_pwd, publish_to)

        step.add_output(name='processed-image', description=outgoing_msg)


# LISTENS TO subscribe_to AND RUNS callback for each incomming message
asyncio.run( aio_subscribe(callback, amqp_host, amqp_user, amqp_pwd, subscribe_to) )
