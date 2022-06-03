import logging

from build.gen.bakdata.lobbyist.v2.lobbyist_pb2 import Lobbyist
from project_utilities.generic_project_producer import GenericProjectProducer
from project_utilities.constant import LOBBYISM_TOPIC

log = logging.getLogger(__name__)


class LrProducer(GenericProjectProducer):
    def __init__(self):
        super().__init__(Lobbyist, LOBBYISM_TOPIC)

    def produce_to_topic(self, message: Lobbyist):
        self.producer.produce(
            topic=LOBBYISM_TOPIC, partition=-1, key=message.lobbyist_id, value=message, on_delivery=self.delivery_report
        )
        # It is a naive approach to flush after each produce this can be optimised
        self.producer.poll()
