from confluent_kafka import Message


class MessageStakeholder:
    def __init__(self, topic: str, group_id: str, message_schema):
        self.topic = topic
        self.group_id = group_id
        self.message_schema = message_schema

    def get_topic(self):
        return self.topic

    def get_group_id(self):
        return self.group_id

    def get_message_schema(self):
        return self.message_schema

    def handle_message(self, message: Message):
        raise NotImplemented
