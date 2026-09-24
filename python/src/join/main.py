import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.fruit_top_by_query: dict[str, list[fruit_item.FruitItem]] = {}
        self.count_parcial_top_by_query: dict[str, int] = {}

    def _process_parcial_top(self, query_id, parcial_fruit_top):
        logging.info(f"Received parcial top query_id: {query_id}")
        fruit_top = self.fruit_top_by_query.setdefault(query_id, [])

        for fruit, ammout in parcial_fruit_top:
            logging.info(f"Received parcial top fruit: {fruit} amount: {ammout}")
            bisect.insort(fruit_top, fruit_item.FruitItem(fruit, ammout))

        self.count_parcial_top_by_query[query_id] = self.count_parcial_top_by_query.get(query_id, 0) + 1

        if self.count_parcial_top_by_query[query_id] == AGGREGATION_AMOUNT:
            final_fruit_top = self.fruit_top_by_query.pop(query_id, [])[-TOP_SIZE:]
            final_fruit_top.reverse()
            send_fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    final_fruit_top
                )
            )

            self.output_queue.send(message_protocol.internal.serialize_top([query_id, send_fruit_top]))

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        command , fields = message_protocol.internal.deserialize(message)

        if command == message_protocol.internal.Command.PARCIAL_TOP:
            self._process_parcial_top(*fields)
        
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
