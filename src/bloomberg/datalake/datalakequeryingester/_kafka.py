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


from __future__ import annotations

import logging
import os
from typing import Any

from confluent_kafka import KafkaException, Producer

KAFKA_TOPIC = os.getenv("DATALAKEQUERYINGESTER_KAFKA_TOPIC")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")


class KafkaProducer:
    def __init__(self) -> None:
        self._topic = KAFKA_TOPIC

        conf = {
            "metadata.broker.list": KAFKA_BROKERS,
            "client.id": self._topic,
            "log.connection.close": False,
            "queue.buffering.max.ms": 100,
            "enable.idempotence": True,
            "message.timeout.ms": 600000,
            "compression.type": "snappy",
        }

        self._producer = Producer(conf)
        logging.info("Created producer")

    def enqueue_message(self, message: bytes) -> None:
        try:
            logging.info("Producing message")
            self._producer.produce(self._topic, message, callback=_delivery_callback)
        except KafkaException as e:
            if e.args[0].fatal():
                logging.fatal(
                    f"ABORTING: Fatal confluent_kafka error caught on produce: {e}",
                    exc_info=True,
                )
                raise
            else:
                logging.error(f"Produce failed: {e}", exc_info=True)

        self._producer.poll()


def _delivery_callback(err: Any, msg: Any) -> None:
    if err:
        # NOTE: In production, this is a condition you should alarm on
        # This is a permanent failure. confluent_kafka has made all configured attempts
        # to get this message delivered.
        logging.error(
            f"Message delivery failed for Message ["
            f" length = {len(msg)} bytes"
            f", topic = {msg.topic()}"
            f", partition = {msg.partition()}"
            f", key = {msg.key()}"
            f" ]: {err}"
        )
    else:
        logging.debug(
            f"Message delivery successful for Message ["
            f" length = {len(msg)} bytes"
            f", topic = {msg.topic()}"
            f", partition = {msg.partition()}"
            f", key = {msg.key()}"
            f", offset = {msg.offset()}"
            f", timestamp = {msg.timestamp()[1]} UTC"
            f" ]"
        )
