import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges: list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_query: dict[str, dict[str, fruit_item.FruitItem]] = {}

        self.input_queue_coordination = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{SUM_PREFIX}_{ID}"
        )

        self.output_queues_coordination: dict[int, middleware.MessageMiddlewareQueueRabbitMQ] = {}
        for i in range(SUM_AMOUNT):
            if i == ID:
                continue
            self.output_queues_coordination[i] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_{i}"
            )
        
        self.count_massages_by_query: dict[str, int] = {}
        self.total_count_messages_by_query: dict[str, int] = {}
        self.response_count_messages_by_query: dict[str, dict[int, int]] = {}
        self.lock_output_queues_coordination = threading.Lock()
        self.lock_query = threading.Lock()

    def _process_data(self, query_id, fruit, amount):
        logging.info(f"Process data")
        # CREO Q ACA SE NECESITA UN LOCK (TIPO LOCK_1) PARA EL TEMA DE LA LECUTRA/ESCRITURA DE COUNT_MESSAGES_BY_QUERY Y EL TEMA DE LA LECTURA/ESCRITURA DE AMOUNT_BY_QUERY
        with self.lock_query:
            self.count_massages_by_query[query_id] = self.count_massages_by_query.get(query_id, 0) + 1
            amount_by_fruit = self.amount_by_query.setdefault(query_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, query_id, total_count_messages):
        self.total_count_messages_by_query[query_id] = total_count_messages
        # ACA CREO Q SE NECESITA UN LOCK (TIPO LOCK_2) PARA EL TEMA DEL LA PUBBLICACION DE MENSAJES EN LA COLA
        with self.lock_output_queues_coordination:
            if len(self.output_queues_coordination) == 0:
                self._process_check_eof_confirm(query_id)
            else:
                for _, output_queue_coordination in self.output_queues_coordination.items():
                    output_queue_coordination.send(
                        message_protocol.internal.serialize_check_eof_readiness(
                            [query_id, ID]
                        )
                    )

    def process_data_messsage(self, message, ack, nack):
        command, fields = message_protocol.internal.deserialize(message)
        if command == message_protocol.internal.Command.RECORD:
            self._process_data(*fields)
        elif command == message_protocol.internal.Command.EOF:
            self._process_eof(*fields)
        ack()


    def _send_fruits(self, query_id, fruits: dict[str, fruit_item.FruitItem]):
        logging.info(f"Broadcasting data messages")
        
        for final_fruit_item in fruits.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize_record(
                        [query_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize_eof([query_id]))

    def _process_check_eof_readiness(self, query_id, coordinator_id):
        # ACA NECESITO DEL LOCK_1 PARA EL TEMA DE LA LECTURA/ESCRITURA DE COUNT_MESSAGES_BY_QUERY
        with self.lock_query:
            count_messages = self.count_massages_by_query.get(query_id, 0)
        # ACA CREO Q SE NECESITA UN LOCK_2 PARA EL TEMA DEL LA PUBBLICACION DE MENSAJES EN LA COLA
        with self.lock_output_queues_coordination:
            self.output_queues_coordination[coordinator_id].send(
                message_protocol.internal.serialize_check_eof_response(
                    [query_id, ID, count_messages]
                )
            )

    def _process_check_eof_response(self, query_id, sum_id, count_messages):

        responses_query = self.response_count_messages_by_query.setdefault(query_id, {})
        responses_query[sum_id] = count_messages

        check_eof_confirm = False
        check_eof_retry = True

        if len(responses_query) < SUM_AMOUNT - 1:
            return

        with self.lock_query:
            total_coordination = sum(responses_query.values()) + self.count_massages_by_query.get(query_id, 0)
        total_expected = self.total_count_messages_by_query.get(query_id, 0)

        self.response_count_messages_by_query.pop(query_id, None)

        if total_coordination == total_expected:
            check_eof_confirm = True
            check_eof_retry = False

        if check_eof_confirm:
            with self.lock_output_queues_coordination:
                for _, output_queue_coordination in self.output_queues_coordination.items():
                    output_queue_coordination.send(message_protocol.internal.serialize_check_eof_confirm([query_id]))
                self._process_check_eof_confirm(query_id)

        if check_eof_retry:
            # PREGUNTAR SI CONVIENE AGREGAR UN SLEEP PARA DAR TIEMPO PARA QUE SE PROCESEN LOS MENSAJES Y EVITAR SE HAGAN PREGUNTAS REDUNTANTES
            with self.lock_output_queues_coordination:
                for _, output_queue_coordination in self.output_queues_coordination.items():
                    output_queue_coordination.send(message_protocol.internal.serialize_check_eof_readiness([query_id, ID]))

    def _process_check_eof_confirm(self, query_id):
        with self.lock_query:
            self.count_massages_by_query.pop(query_id, None)
            self.total_count_messages_by_query.pop(query_id, None)
            fruits = self.amount_by_query.pop(query_id, {})
        self._send_fruits(query_id, fruits)

    def process_coordination_message(self, message, ack, nack):
        command, fields = message_protocol.internal.deserialize(message)
        if command == message_protocol.internal.Command.CHECK_EOF_READINESS:
            self._process_check_eof_readiness(*fields)
        elif command == message_protocol.internal.Command.CHECK_EOF_RESPONSE:
            self._process_check_eof_response(*fields)
        elif command == message_protocol.internal.Command.CHECK_EOF_CONFIRM:
            self._process_check_eof_confirm(*fields)
        ack()

    def _start_coordination(self):
        self.input_queue_coordination.start_consuming(self.process_coordination_message)
        

    def start(self):
        thread_coordination = threading.Thread(target=self._start_coordination)
        thread_coordination.start()
        self.input_queue.start_consuming(self.process_data_messsage)
        thread_coordination.join()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
