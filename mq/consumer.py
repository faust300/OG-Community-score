import pika, os, functools, json
import dotenv
from functools import partial
from doc2vec import HotScore

dotenv.load_dotenv(verbose=True)
EXCHANGE = 'og.community'
QUEUE = 'og.score.post'
HOST = os.environ['MQ_HOST']
PORT = os.environ['MQ_PORT']
CREDENCIAL = pika.PlainCredentials(os.environ['MQ_ID'], os.environ['MQ_PW'])

class consumer:
    def __init__(self):
        self.url = HOST
        self.port = PORT
        self.credentials = CREDENCIAL
        self.heartbeat = 60
        return
    
    def on_open(self, connection):
        print('consumer open')
        return
    
    def on_message(self, channel, method, properties, body):
        """Called when a message is received. Log message and ack it."""
        print("Received {}".format(body))
        json_data = json.loads(body)
        print(json_data['postId'])
        hot = HotScore()
        hot.main(json_data['postId'])
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return json_data

    def main(self):
        param = pika.ConnectionParameters(
            host=self.url,
            port=self.port,
            heartbeat=self.heartbeat,
            credentials=self.credentials,
        )
        print('consumer start')
        conn = pika.BlockingConnection(param)
        print('consumer connect')
        channel = conn.channel()
        print('consumer connected')
        
        on_message_callback = functools.partial(self.on_message)
        channel.basic_consume('og.score.post', on_message_callback)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        conn.close()