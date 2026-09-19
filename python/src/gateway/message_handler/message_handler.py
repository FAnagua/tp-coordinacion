from common import message_protocol
import uuid
import logging

class MessageHandler:

    def __init__(self):
        self.query_id = str(uuid.uuid4())
        
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize_record([self.query_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize_eof([self.query_id])

    def deserialize_result_message(self, message):
        query_id, top_fruit = message_protocol.internal.deserialize(message)
        
        # logging.error(f"Deserializing result message: {query_id}, {top_fruit}")
        # logging.error(f"Current query id: {self.query_id}")
        if query_id != self.query_id:
            return None

        return top_fruit
