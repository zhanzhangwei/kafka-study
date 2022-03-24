from confluent_kafka import Producer
from random import randint
import json

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewPartitions


class KafkaProducerTool(object):

    def __init__(self, topic, broker):
        config = {
            'bootstrap.servers': broker,
        }
        self.topic = topic
        self.client = Producer(config)
        self.admin = AdminClient(config)
        # 初始化创建分区99个
        new_partitions = [NewPartitions(topic=self.topic, new_total_count=99)]
        self.admin.create_partitions(new_partitions=new_partitions)

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def send_msg(self, msg):
        """
        生产数据到指定分区
        :param msg:
        :return:
        """
        self.client.produce(self.topic, msg.encode('utf-8'), partition=4, callback=self.delivery_report)


if __name__ == '__main__':

    broker = "39.108.187.214:9092"
    topic = "mytopic"
    p = KafkaProducerTool(topic, broker)
    some_data_source = []
    for i in range(5):
        some_data_source.append({
            'name': randint(0, 10),
            'age': randint(10, 20),
            'sex': randint(30, 40),
            'partition': 2
        })
    # NewPartitions({'topic': 'mytopic', 'new_total_cnt': 2, replica_assignment=None})

    for data in some_data_source:
        p.client.poll(0)
        p.send_msg(msg=json.dumps(data))
    p.client.flush()