from common import message_protocol
import uuid
import logging

class MessageHandler:

    def __init__(self):
        self.query_id = str(uuid.uuid4())
        self.total_count_messages = 0
        
    
    def serialize_data_message(self, message):
        self.total_count_messages += 1
        [fruit, amount] = message
        return message_protocol.internal.serialize_record([self.query_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize_eof([self.query_id, self.total_count_messages])

    def deserialize_result_message(self, message):
        command, [query_id, top_fruit] = message_protocol.internal.deserialize(message)
        
        
        if command == message_protocol.internal.Command.TOP and query_id == self.query_id:
            return top_fruit

        return None
