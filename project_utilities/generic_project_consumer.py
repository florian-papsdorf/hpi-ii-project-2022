import logging

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer

from project_utilities.constant import BOOTSTRAP_SERVER
from project_utilities.message_stakeholder import MessageStakeholder

log = logging.getLogger(__name__)


class GenericProjectConsumer:
    def __init__(self, message_stakeholder: MessageStakeholder):
        self.message_stakeholder = message_stakeholder

        protobuf_deserializer = ProtobufDeserializer(
            self.message_stakeholder.get_message_schema(), {"use.deprecated.format": True}
        )

        consumer_conf = {
            "bootstrap.servers": BOOTSTRAP_SERVER,
            'group.id': self.message_stakeholder.get_group_id(),
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": protobuf_deserializer,
        }

        self.consumer = DeserializingConsumer(consumer_conf)
        self.consumer.subscribe([self.message_stakeholder.get_topic()])

    def listen(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            self.message_stakeholder.handle_message(message=msg)
