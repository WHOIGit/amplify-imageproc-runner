import os
import json
from io import BytesIO

import pika
import requests

from s3 import s3_upload, s3_download, bucketkey
from provenance.capture import Logger, Step
#from provenance.amqp import amqp_subscribe, amqp_publish

subscribe_to = os.getenv('AMQP_EXCHANGE_SUBSCRIBE','upload')
publish_to = os.getenv('AMQP_EXCHANGE_PUBLISH', 'dithered')
output_bucket = os.getenv('S3_OUTPUT_BUCKET', 'amplify-poc-output')

amqp_host = os.environ.get('AMQP_HOST', 'localhost')
amqp_exchange = os.environ.get('AMQP_EXCHANGE_PROV', 'provenance')



def amqp_subscribe(host, exchange_name, callback):
    creds = pika.PlainCredentials('myrabbitmquser', 'myrabbitmqpassword')

    conn_params = pika.ConnectionParameters(host,credentials=creds)
    print(creds, conn_params)
    
    connection = pika.BlockingConnection(conn_params)
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange=exchange_name, queue=queue_name)

    def on_message(ch, method, properties, body):
        callback(json.loads(body))

    channel.basic_consume(queue=queue_name,
                          on_message_callback=on_message,
                          auto_ack=True)

    channel.start_consuming()


def amqp_publish(host, exchange_name, message):
    creds = pika.PlainCredentials(os.getenv('RABBITMQ_USER','guest'), os.getenv('RABBITMQ_PASSWORD','guest'))
    conn_params = pika.ConnectionParameters(host,credentials=creds)
    connection = pika.BlockingConnection(conn_params)
    channel = connection.channel()


    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')

    channel.basic_publish(exchange=exchange_name,
                          routing_key='',
                          body=json.dumps(message))

    connection.close()




def callback(msg):

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
        for chunk in resp.iter_content():
            content_out.extend(chunk)
    print('IMG DITHERED')
     
    # upload results to s3
    output_key = f'dither/{msg["key"]}'
    s3_upload(output_bucket, output_key, content_out)
    print('UPLOADED:', output_key)
    
    # log provenance
    print('TODO LOG PROVENENCE')
    
    # push to amqp
    outgoing_msg = dict(type='imageproc', bucket=output_bucket, key=output_key)
    amqp_publish(amqp_host, publish_to, outgoing_msg)
    print('PUBLISHED:', outgoing_msg)


print('HELLO WORLD')

# LISTENS TO subscribe_to AND RUNS callback for each incomming message
amqp_subscribe(amqp_host, subscribe_to, callback)




