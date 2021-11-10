"""
 ** Copyright 2021 Bloomberg Finance L.P.
 **
 ** Licensed under the Apache License, Version 2.0 (the "License");
 ** you may not use this file except in compliance with the License.
 ** You may obtain a copy of the License at
 **
 **     http://www.apache.org/licenses/LICENSE-2.0
 **
 ** Unless required by applicable law or agreed to in writing, software
 ** distributed under the License is distributed on an "AS IS" BASIS,
 ** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ** See the License for the specific language governing permissions and
 ** limitations under the License.
"""


import json
import logging

from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger()


class KafkaConsumer:
    def __init__(self, queue, expected_event_count=1):
        self.queue = queue
        event_count = 0
        topic = "kf-datalake-query-events"
        consumer_config = {
            "metadata.broker.list": "local_kafka:9092",
            "client.id": "kf-cna-model-event-test-consumer",
            "enable.auto.offset.store": True,
            "log.connection.close": False,
            "enable.partition.eof": False,
            "group.id": "cna",
            "auto.offset.reset": "smallest",
        }

        self._consumer = Consumer(consumer_config)
        self._consumer.subscribe([topic])

        while event_count < expected_event_count:
            message = self._consumer.poll(timeout=1.0)
            if message is None:
                pass
            elif message.error():
                if message.error().fatal():
                    logger.error(f"ABORTING: Fatal confluent_kafka error caught on consume: {message.error()}")
                    raise KafkaException(message.error())
                elif message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.error(f"Permanent consumer error: {message.error()}")
                else:
                    logger.info(f"Non-critical consume event: {message.error()}")
            else:
                event_count += 1
                self.handle_message(message)

        # self._consumer.close()

    def handle_message(self, message):
        try:
            payload = json.loads(message.value())
            logger.debug(
                "Received Message"
                + "[ length = %s bytes, topic = %s, partition = %s, key = %s, offset = %s, timestamp = %s UTC ]",
                len(message),
                message.topic(),
                message.partition(),
                message.key(),
                message.offset(),
                message.timestamp()[1],
            )
            self.queue.put(payload)
        except (ValueError, json.decoder.JSONDecodeError, TypeError) as exc:
            logger.error(
                "Error parsing JSON for Message"
                + "[ length = %s bytes, topic = %s, partition = %s, key = %s, offset = %s, timestamp = %s UTC ]: %s",
                len(message),
                message.topic(),
                message.partition(),
                message.key(),
                message.offset(),
                message.timestamp()[1],
                exc,
            )
            self.queue.put(None)
