import json
from enum import IntEnum

class Command(IntEnum):
    RECORD = 0X01
    EOF = 0X02
    CHECK_EOF_READINESS = 0X03
    CHECK_EOF_RESPONSE = 0X04
    CHECK_EOF_CONFIRM = 0X05

def serialize_record(message):
    return json.dumps([Command.RECORD, message]).encode("utf-8")

def serialize_eof(message):
    return json.dumps([Command.EOF, message]).encode("utf-8")

def serialize_check_eof_readiness(message):
    return json.dumps([Command.CHECK_EOF_READINESS, message]).encode("utf-8")

def serialize_check_eof_response(message):
    return json.dumps([Command.CHECK_EOF_RESPONSE, message]).encode("utf-8")

def serialize_check_eof_confirm(message):
    return json.dumps([Command.CHECK_EOF_CONFIRM, message]).encode("utf-8")

def serialize(message):
    return json.dumps(message).encode("utf-8")

def deserialize(message):
    return json.loads(message.decode("utf-8"))
