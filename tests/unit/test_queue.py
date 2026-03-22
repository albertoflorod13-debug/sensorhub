import json
from unittest.mock import MagicMock, call, patch


@patch("sensorhub.queue.pika")
def test_publish_sends_message(mock_pika):
    mock_channel = MagicMock()
    mock_conn = MagicMock()
    mock_conn.channel.return_value = mock_channel
    mock_pika.BlockingConnection.return_value = mock_conn

    from sensorhub.queue import publish
    msg = {"device_id": "s1", "temperature": 22.5}
    publish(msg)

    mock_channel.queue_declare.assert_called_once_with(queue="sensor.readings", durable=True)
    mock_channel.basic_publish.assert_called_once()
    _, kwargs = mock_channel.basic_publish.call_args
    assert kwargs["routing_key"] == "sensor.readings"
    assert json.loads(kwargs["body"]) == msg
    mock_pika.BasicProperties.assert_called_once_with(delivery_mode=2)
    mock_conn.close.assert_called_once()


@patch("sensorhub.queue.pika")
def test_publish_closes_connection_on_success(mock_pika):
    mock_conn = MagicMock()
    mock_pika.BlockingConnection.return_value = mock_conn

    from sensorhub.queue import publish
    publish({"device_id": "s1", "temperature": 22.5})

    mock_conn.close.assert_called_once()
