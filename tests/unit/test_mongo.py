from unittest.mock import MagicMock, patch


@patch("sensorhub.mongo.MongoClient")
def test_insert_many_returns_count(mock_client_cls):
    mock_collection = MagicMock()
    mock_collection.insert_many.return_value.inserted_ids = ["id1", "id2", "id3"]
    mock_client_cls.return_value.get_database.return_value.get_collection.return_value = mock_collection

    from sensorhub.mongo import MongoDB
    db = MongoDB()
    docs = [{"device_id": f"s{i}"} for i in range(3)]
    count = db.insert_many(docs)

    assert count == 3
    mock_collection.insert_many.assert_called_once_with(docs)


@patch("sensorhub.mongo.MongoClient")
def test_insert_many_empty(mock_client_cls):
    mock_collection = MagicMock()
    mock_collection.insert_many.return_value.inserted_ids = []
    mock_client_cls.return_value.get_database.return_value.get_collection.return_value = mock_collection

    from sensorhub.mongo import MongoDB
    db = MongoDB()
    count = db.insert_many([])

    assert count == 0
