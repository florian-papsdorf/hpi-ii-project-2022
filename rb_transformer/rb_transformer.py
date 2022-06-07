import logging
import os

from project_utilities.message_stakeholder import MessageStakeholder
from project_utilities.constant import CORPORATE_TOPIC, CORPORATE_DETAILED_TOPIC
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate
from build.gen.bakdata.corporate_detailed.v1.corporate_detailed_pb2 import Corporate_Detailed
from project_utilities.generic_project_consumer import GenericProjectConsumer
from project_utilities.generic_project_producer import GenericProjectProducer

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


class RbTransformer(MessageStakeholder):
    class RbConsumer(GenericProjectConsumer):
        def __init__(self, message_stakeholder):
            super().__init__(message_stakeholder)

        def consume(self):
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                self.message_stakeholder.handle_message(msg)

    class RbDetailedProducer(GenericProjectProducer):
        def __init__(self):
            super().__init__(Corporate_Detailed, CORPORATE_DETAILED_TOPIC)

        def produce_to_topic(self, message: Corporate_Detailed):
            self.producer.produce(
                topic=CORPORATE_DETAILED_TOPIC, partition=-1, key=str(message.id), value=message,
                on_delivery=self.delivery_report
            )
            # It is a naive approach to flush after each produce this can be optimised
            self.producer.poll()

    def __init__(self):
        super(RbTransformer, self).__init__(topic=CORPORATE_TOPIC, group_id="rb_transformer", message_schema=Corporate)
        self.rb_consumer = self.RbConsumer(self)
        self.producer = self.RbDetailedProducer()
        self.rb_consumer.consume()

    @staticmethod
    def extract_company_name_city(information_field: str):
        # parse by <ref_number>: <corporate_name>,<city>,<address>,<zip_code>,<city>
        if information_field.startswith("HRB"):
            index_end_ref_number = information_field.find(":")
        else:
            index_end_ref_number = -1
        index_end_corporate_name = information_field.find(",")
        interesting_right_end = information_field[index_end_corporate_name + 1:]
        index_end_city = interesting_right_end.find(",")
        return information_field[index_end_ref_number + 1:index_end_corporate_name], interesting_right_end[
                                                                                     0:index_end_city]

    @staticmethod
    def extract_persons(information_field: str, persons: list):
        person_indicators = ["Geschäftsführer", "Einzelprokura", "Prokura erloschen", "Liquidator", "(Vorsitzender)",
                             "Vorsitzender", "Leitungsorgan", "Inhaber", "Gesellschafter"]
        indices = list(
            filter(lambda x: x >= 0, list(map(lambda token: information_field.find(token), person_indicators))))
        if len(indices) == 0:
            return persons
        important_end = information_field[min(indices):]
        important_end = important_end[important_end.find(":"):]
        name_parts = list()
        for _ in range(0, 2):
            delimiter = important_end.find(",")
            name_parts.append(important_end[1:delimiter])
            important_end = important_end[delimiter + 1:]
        # standardization firstname, lastname
        name_parts.reverse()
        person_name = " ".join(name_parts)
        # normalization: remove obsolete appendices
        name_appendices = ["Dr. ", "Dr ", "Prof. ", "Prof "]
        for appendix in name_appendices:
            person_name = person_name.replace(appendix, "")
        if person_name[0] == " ":
            person_name = person_name[1:]
        if len(person_name) < 25:
            persons.append(person_name)
        return RbTransformer.extract_persons(important_end, persons)

    def handle_message(self, message):
        corporate = message.value()
        detailed_corporate = Corporate_Detailed()
        detailed_corporate.id = corporate.id
        detailed_corporate.rb_id = corporate.rb_id
        detailed_corporate.state = corporate.state
        detailed_corporate.company_name, detailed_corporate.city = RbTransformer.extract_company_name_city(
            corporate.information)
        detailed_corporate.event_date = corporate.event_date
        detailed_corporate.event_type = corporate.event_type
        detailed_corporate.status = corporate.status
        detailed_corporate.information = corporate.information
        detailed_corporate.reference_id = corporate.reference_id
        detailed_corporate.persons.extend(RbTransformer.extract_persons(corporate.information, list()))
        self.producer.produce_to_topic(detailed_corporate)


if __name__ == '__main__':
    sample_rb_transformer = RbTransformer()
