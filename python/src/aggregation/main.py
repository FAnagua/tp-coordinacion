import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_by_query: dict[str, dict[str, fruit_item.FruitItem]] = {}
        self.fruit_top_by_query: dict[str, list[fruit_item.FruitItem]] = {}
        self.count_eof_by_query: dict[str, int] = {}

    def _process_data(self, query_id, fruit, amount):
        logging.info("Processing data message")
        ammount_by_fruit = self.fruit_by_query.setdefault(query_id, {})
        fruit_top = self.fruit_top_by_query.setdefault(query_id, [])

        if fruit in ammount_by_fruit:
            old_fruit = ammount_by_fruit[fruit]
            fruit_top.remove(old_fruit)

            new_fruit = old_fruit + fruit_item.FruitItem(fruit, int(amount))
            ammount_by_fruit[fruit] = new_fruit

            bisect.insort(fruit_top, new_fruit)
        else:
            new_fruit = fruit_item.FruitItem(fruit, int(amount))
            ammount_by_fruit[fruit] = new_fruit
            bisect.insort(fruit_top, new_fruit)

    def _process_eof(self, query_id):
        logging.info(f"Received EOF query_id: {query_id}")
        self.count_eof_by_query[query_id] = self.count_eof_by_query.get(query_id, 0) + 1

        if self.count_eof_by_query[query_id] == SUM_AMOUNT:
            fruit_chunk = self.fruit_top_by_query.pop(query_id, [])[-TOP_SIZE:]
            fruit_chunk.reverse()
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_chunk,
                )
            )
            self.output_queue.send(message_protocol.internal.serialize_record([query_id, fruit_top]))
        #del self.fruit_top_by_query[query_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        command ,fields = message_protocol.internal.deserialize(message)
        if command == message_protocol.internal.Command.RECORD:
            self._process_data(*fields)
        elif command == message_protocol.internal.Command.EOF:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
