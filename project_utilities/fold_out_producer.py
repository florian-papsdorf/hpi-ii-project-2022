from project_utilities.generic_project_producer import GenericProjectProducer
from build.gen.bakdata.fold_out.v1.fold_out_pb2 import Fold_out
from project_utilities.constant import FOLD_OUT_TOPIC


class FoldOutProducer(GenericProjectProducer):
    def __init__(self):
        super().__init__(Fold_out, FOLD_OUT_TOPIC)

    def produce_to_topic(self, message: Fold_out):
        self.producer.produce(
            topic=FOLD_OUT_TOPIC, partition=-1, key=str(message.continuous_id), value=message,
            on_delivery=self.delivery_report
        )
        # It is a naive approach to flush after each produce this can be optimised
        self.producer.poll()
