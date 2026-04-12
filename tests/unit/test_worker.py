import json
from unittest.mock import MagicMock, patch

import pytest

from sensorhub.worker import _parse_document, BATCH_SIZE, FLUSH_INTERVAL


# ── _parse_document ────────────────────────────────────────────────────────────

def test_parse_document_with_timestamp():
    msg = {
        "device_id": "s1",
        "location": "office",
        "temperature": "22.5",
        "humidity": "55.0",
        "co2": "400",
        "timestamp": "2026-01-01T10:00:00",
    }
    doc = _parse_document(msg)
    assert doc["device_id"] == "s1"
    assert doc["temperature"] == 22.5
    assert doc["humidity"] == 55.0
    assert doc["co2"] == 400
    assert doc["source"] == "queue"
    assert doc["timestamp"].year == 2026


def test_parse_document_without_timestamp():
    msg = {"device_id": "s1", "location": "office", "temperature": 22.5, "humidity": 55.0, "co2": 400}
    doc = _parse_document(msg)
    assert doc["timestamp"] is not None
    assert doc["source"] == "queue"


def test_parse_document_coerces_types():
    msg = {"device_id": "s1", "location": "x", "temperature": "21", "humidity": "50", "co2": "399"}
    doc = _parse_document(msg)
    assert isinstance(doc["temperature"], float)
    assert isinstance(doc["co2"], int)
    assert doc["co2"] == 399


# ── run() helpers ──────────────────────────────────────────────────────────────

def _make_message(i: int) -> tuple:
    method = MagicMock()
    method.delivery_tag = i
    body = json.dumps({
        "device_id": f"s{i}",
        "location": "office",
        "temperature": 22.0,
        "humidity": 50.0,
        "co2": 400,
        "timestamp": "2026-01-01T10:00:00",
    }).encode()
    return method, None, body


def _make_run_mocks(messages):
    mock_channel = MagicMock()
    mock_channel.consume.return_value = iter(messages)
    mock_conn = MagicMock()
    mock_conn.channel.return_value = mock_channel
    mock_db = MagicMock()
    mock_db.insert_many.return_value = len([m for m in messages if m[0] is not None])
    return mock_conn, mock_channel, mock_db


# ── flush por batch ────────────────────────────────────────────────────────────

@patch("sensorhub.worker.MongoDB")
@patch("sensorhub.worker.pika")
def test_flush_on_batch_size(mock_pika, mock_mongodb_cls):
    messages = [_make_message(i) for i in range(BATCH_SIZE)]
    mock_conn, mock_channel, mock_db = _make_run_mocks(messages)
    mock_pika.BlockingConnection.return_value = mock_conn
    mock_mongodb_cls.return_value = mock_db

    from sensorhub import worker
    worker.run()

    mock_db.insert_many.assert_called_once()
    docs = mock_db.insert_many.call_args[0][0]
    assert len(docs) == BATCH_SIZE


@patch("sensorhub.worker.MongoDB")
@patch("sensorhub.worker.pika")
def test_ack_called_for_each_message(mock_pika, mock_mongodb_cls):
    messages = [_make_message(i) for i in range(3)]
    mock_conn, mock_channel, mock_db = _make_run_mocks(messages)
    mock_pika.BlockingConnection.return_value = mock_conn
    mock_mongodb_cls.return_value = mock_db

    from sensorhub import worker
    worker.run()

    assert mock_channel.basic_ack.call_count == 3


# ── flush por tiempo ───────────────────────────────────────────────────────────

@patch("sensorhub.worker.time")
@patch("sensorhub.worker.MongoDB")
@patch("sensorhub.worker.pika")
def test_flush_on_time_interval(mock_pika, mock_mongodb_cls, mock_time):
    # 1 mensaje real + 1 timeout (method=None)
    messages = [_make_message(0), (None, None, None)]
    mock_conn, mock_channel, mock_db = _make_run_mocks(messages)
    mock_pika.BlockingConnection.return_value = mock_conn
    mock_mongodb_cls.return_value = mock_db
    # Simula que ha pasado más de FLUSH_INTERVAL desde el último flush
    mock_time.time.side_effect = [0, FLUSH_INTERVAL + 1, FLUSH_INTERVAL + 1]

    from sensorhub import worker
    worker.run()

    mock_db.insert_many.assert_called_once()


@patch("sensorhub.worker.time")
@patch("sensorhub.worker.MongoDB")
@patch("sensorhub.worker.pika")
def test_no_flush_if_interval_not_reached(mock_pika, mock_mongodb_cls, mock_time):
    messages = [_make_message(0), (None, None, None)]
    mock_conn, mock_channel, mock_db = _make_run_mocks(messages)
    mock_pika.BlockingConnection.return_value = mock_conn
    mock_mongodb_cls.return_value = mock_db
    # No ha pasado suficiente tiempo
    mock_time.time.return_value = 0

    from sensorhub import worker
    worker.run()

    mock_db.insert_many.assert_not_called()
