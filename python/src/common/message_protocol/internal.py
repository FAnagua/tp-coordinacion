import json
from enum import IntEnum

class Command(IntEnum):
    RECORD = 0X01
    EOF = 0X02

def serialize_record(message):
    return json.dumps([Command.RECORD, message]).encode("utf-8")

def serialize_eof(message):
    return json.dumps([Command.EOF, message]).encode("utf-8")

def serialize(message):
    return json.dumps(message).encode("utf-8")

def deserialize(message):
    return json.loads(message.decode("utf-8"))
