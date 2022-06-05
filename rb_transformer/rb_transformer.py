import logging

from project_utilities.message_stakeholder import MessageStakeholder
from project_utilities.constant import CORPORATE_TOPIC
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate
from project_utilities.generic_project_consumer import GenericProjectConsumer

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

    def __init__(self):
        super(RbTransformer, self).__init__(topic=CORPORATE_TOPIC, group_id="rb_transformer", message_schema=Corporate)
        self.rb_consumer = self.RbConsumer(self)
        self.rb_consumer.consume()

    def handle_message(self, message):
        corporate = message.value()
        print(corporate.information)


if __name__ == '__main__':
    sample_rb_transformer = RbTransformer()

