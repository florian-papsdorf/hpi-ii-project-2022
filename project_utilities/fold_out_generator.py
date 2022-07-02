from build.gen.bakdata.fold_out.v1.fold_out_pb2 import Fold_out, RELATION
from project_utilities.conitnuous_id_generator import ContinuousIDGenerator
from project_utilities.fold_out_producer import FoldOutProducer


class FoldOutBuilder:
    def __init__(self, prefix: str):
        self.fold_out_id_generator = ContinuousIDGenerator(prefix)
        self.fold_out_producer = FoldOutProducer()
        self.prefix = prefix

    def build_fold_out(self, company_name: str, s_id: str, relation: RELATION):
        fold_out_message = Fold_out()
        fold_out_message.continuous_id = self.fold_out_id_generator.get_next_identifier()
        fold_out_message.company_name = company_name
        fold_out_message.source_id = s_id
        fold_out_message.source_prefix = self.prefix
        fold_out_message.relation = relation
        self.fold_out_producer.produce_to_topic(fold_out_message)
