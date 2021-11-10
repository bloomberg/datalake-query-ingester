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


from queue import Queue
from threading import Thread

import pytest
import requests

from .kafka_consumer import KafkaConsumer


@pytest.fixture()
def _consume_one_event():
    queue = Queue()
    expected_event_count = 1
    consumer = Thread(target=KafkaConsumer, args=(queue, expected_event_count))

    yield

    consumer.start()
    consumer.join()


def test_healthy() -> None:
    # When
    response = requests.get("http://datalakequeryingester:8080/healthy")

    # Then
    assert response.status_code == 200
    assert response.json()["message"] == "healthy"


@pytest.mark.usefixtures("_consume_one_event")
def test_add_query_ingest():
    # Given
    request = {"query": "select * from table;"}

    # When
    response = requests.post("http://datalakequeryingester:8080/add_query_ingest", json=request)

    # Then
    assert response.status_code == 200
    assert response.json() == {"ok": True}
