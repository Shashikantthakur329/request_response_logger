# middleware_module.py

from flask import request
from confluent_kafka import Producer
import json
import time

messages = []

class KafkaMiddleware:
    def __init__(self, app, topic, akto_account_id, limit):
        self.app = app
        self.topic = topic
        self.akto_account_id = akto_account_id
        self.limit = limit
        self.producer = self.get_kafka_producer('your_client_id', 'your_broker')

    def __call__(self, environ, start_response):
        chunks = []

        def custom_write(data, *args, **kwargs):
            chunks.append(data)
            old_write(data, *args, **kwargs)

        def custom_end(data=None, *args, **kwargs):
            if data:
                chunks.append(data)

            try:
                content_type = response_headers.get('content-type')
                if content_type and 'application/json' in content_type:
                    log_json = self.generate_log(request, response_headers, chunks, self.akto_account_id)
                    messages.append({'value': log_json})
                    l = len(messages)
                    if l >= self.limit:
                        self.send_to_kafka(self.topic, messages[:self.limit])
                        del messages[:self.limit]
            except Exception as e:
                print(e)

            old_end(data, *args, **kwargs)

        old_write = start_response
        old_end = environ['wsgi.input'].close
        environ['wsgi.input'].close = custom_end
        start_response = custom_write

        return self.app(environ, start_response)

    def generate_log(self, req, res, chunks, akto_account_id):
        body = b''.join(chunks).decode('utf-8')
        value = {
            'path': req.path,
            'requestHeaders': json.dumps(dict(req.headers)),
            'responseHeaders': json.dumps(dict(res)),
            'method': req.method,
            'requestPayload': json.dumps(request.json),
            'responsePayload': body,
            'ip': req.headers.get('x-forwarded-for') or req.remote_addr,
            'time': str(int(time.time())),
            'statusCode': res.status,
            'type': f'HTTP/{req.environ.get("SERVER_PROTOCOL")}',
            'status': res.status,
            'akto_account_id': akto_account_id,
            'contentType': res.get('content-type'),
        }

        return json.dumps(value)

    def send_to_kafka(self, topic, messages):
        for message in messages:
            self.producer.produce(topic, value=message['value'])
        self.producer.flush()

    def get_kafka_producer(self, client_id, brokers):
        producer_config = {
            'bootstrap.servers': brokers,
            'client.id': client_id,
        }
        return Producer(producer_config)
