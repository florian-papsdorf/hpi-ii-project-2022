import logging

from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate

from project_utilities.generic_project_producer import GenericProjectProducer
from project_utilities.constant import CORPORATE_TOPIC

log = logging.getLogger(__name__)


class RbProducer(GenericProjectProducer):
    def __init__(self):
        super().__init__(Corporate, CORPORATE_TOPIC)

    def produce_to_topic(self, message: Corporate):
        self.producer.produce(
            topic=CORPORATE_TOPIC, partition=-1, key=str(message.id), value=message, on_delivery=self.delivery_report
        )

        # It is a naive approach to flush after each produce this can be optimised
        self.producer.poll()
