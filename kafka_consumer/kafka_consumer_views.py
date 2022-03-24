from confluent_kafka import Consumer
from confluent_kafka.cimpl import TopicPartition

from conf.common import IP


class KafkaConsumerTool(object):
    def __init__(self, broker, topic, partition, offset):
        config = {
            'bootstrap.servers': broker,
            'group.id': 'test',
            'enable.auto.commit': True,
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        }
        self.client = Consumer(config)
        # 指定消费分区跟偏移量
        self.client.assign([TopicPartition(topic=topic, partition=partition, offset=offset)])
        self.client.subscribe([topic])


if __name__ == '__main__':
    topic = 'mytopic'
    partition = 5
    offset = 100
    c = KafkaConsumerTool(topic=topic, broker=IP, partition=partition, offset=offset)
    while True:
        msg = c.client.poll(1)
        if msg is None:
            continue
        else:
            if not msg.error() is None:
                print(msg.error())
            else:
                message = msg.value()
                print(msg.partition(), msg.offset())
