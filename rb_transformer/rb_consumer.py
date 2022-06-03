import logging

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer

from build.gen.bakdata.corporate.v1 import corporate_pb2
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate
from rb_transformer.constant import BOOTSTRAP_SERVER, INPUT_TOPIC


log = logging.getLogger(__name__)


class RbConsumer:
    def __init__(self):

        protobuf_deserializer = ProtobufDeserializer(
            corporate_pb2.Corporate, {"use.deprecated.format": True}
        )

        consumer_conf = {
            "bootstrap.servers": BOOTSTRAP_SERVER,
            'group.id': 'rb_transformer',
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": protobuf_deserializer,
        }

        self.consumer = DeserializingConsumer(consumer_conf)
        self.consumer.subscribe([INPUT_TOPIC])

    def printer(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            print(msg.key())
            print(msg.value())


if __name__ == '__main__':
    sampleConsumer = RbConsumer()
    sampleConsumer.printer()
