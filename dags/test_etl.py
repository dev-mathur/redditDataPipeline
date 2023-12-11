from datetime import datetime, timedelta
import pytest
import sys
sys.path.append('etl.py')
from etl import extract, transform, load

#Double check the expected input/output of extract method
def test_extract_function(mocker):
    # Patch the gdown.download function
    gdown_mock = mocker.patch('etl.gdown.download')
    gdown_mock.return_value = None

    # Patch ZstdDecompressor and its decompress method
    zstd_mock = mocker.patch('etl.zstd.ZstdDecompressor')
    decompress_mock = zstd_mock.return_value.decompress
    decompress_mock.return_value = b'{"key": "value"}'

    # Patch open function for file operations
    open_mock = mocker.patch('etl.open', mocker.mock_open())

    # Execute the extract function
    result = extract(execution_date='2023-12-11')

    # Assertions and verifications
    gdown_mock.assert_called_once_with(
        "https://drive.google.com/uc?id=1E7iRwCp7IjvCjh_-owrt2NTMWnvgleZp",
        "submissions.zst",
        quiet=False
    )
    open_mock.assert_has_calls([
    mocker.call('submissions.zst', 'rb'),
    mocker.call().__enter__(),
    mocker.call().__exit__(None, None, None),
    mocker.call('ConvertedRawData.json', 'w+'),
    mocker.call().__enter__(),
    mocker.call().write('[{"key": "value"}]'),
    mocker.call().__exit__(None, None, None),
], any_order=True)
    decompress_mock.assert_called_once()
    open_mock().write.assert_called_once_with('[{"key": "value"}]')
    assert result == 'ConvertedRawData.json'

#Double check the expected input/output of transform method
def test_transform_function(mocker):
    # Mocking the 'open' function to simulate file reading
    open_mock = mocker.patch('etl.open', mocker.mock_open(read_data='[{"ingestion_timestamp": "2023-12-11", "data": {"id": 1, "title": "Sample title", "author": "Sample author", "subreddit": "Sample subreddit", "num_comments": 5, "ups": 10, "downs": 2, "upvote_ratio": 0.8, "permalink": "sample_link"}}]'))

    # Execute the transform function
    result = transform('test.json')  # You can pass any dummy file name here

    # Assertions and verifications
    open_mock.assert_called_once_with('test.json')
    open_mock.return_value.__enter__().read.assert_called_once()

    # Ensure the transformation logic works correctly
    assert len(result) == 1
    assert result[0]["datePosted"] == "2023-12-11"
    assert result[0]["id"] == 1
    assert result[0]["title"] == "Sample title"
    assert result[0]["author"] == "Sample author"
    assert result[0]["subreddit"] == "Sample subreddit"
    assert result[0]["postType"] == "None"  # Assuming 'post_hint' is not present
    assert result[0]["num_comments"] == 5
    assert result[0]["num_upvotes"] == 10
    assert result[0]["num_downvotes"] == 2
    assert result[0]["upvote_ratio"] == 0.8
    assert result[0]["post_link"] == "sample_link"

#Double check the expected input/output of transform method
def test_load_function(mocker):
    # Mock MongoClient and its methods
    MongoClient_mock = mocker.patch('etl.MongoClient')

    # Mocking the client and database using MagicMock
    client_mock = mocker.MagicMock()
    client_mock.list_database_names.return_value = ['redditDB']  # Simulate the existence of 'redditDB'

    db_mock = mocker.MagicMock()
    db_mock.list_collection_names.return_value = ['redditPosts']  # Simulate the existence of 'redditPosts'
    
    # Set the return value for __getitem__ to mock the behavior of accessing 'redditDB'
    client_mock.__getitem__.return_value = db_mock

    MongoClient_mock.return_value = client_mock

    test_data = [
    {
        "datePosted": "2023-12-11",
        'id': 1,
        'title': 'Sample title 1',
        'author': 'Author 1',
        'subreddit': 'Subreddit A',
        'postType': 'Link',  # Example post type
        'num_comments': 10,
        'num_upvotes': 20,
        'num_downvotes': 5,
        'upvote_ratio': 0.75,
        'post_link': '/sample_link_1'
    },
    {
        "datePosted": "2023-12-12",
        'id': 2,
        'title': 'Sample title 2',
        'author': 'Author 2',
        'subreddit': 'Subreddit B',
        'postType': 'Image',  # Example post type
        'num_comments': 15,
        'num_upvotes': 25,
        'num_downvotes': 3,
        'upvote_ratio': 0.85,
        'post_link': '/sample_link_2'
    }]
    # Execute the load function
    load(test_data)  # Pass an empty list or data as required

    # Assertions and verifications
    MongoClient_mock.assert_called_once_with("mongodb://mongo:27017/")
    client_mock.list_database_names.assert_called_once()

    client_mock.__getitem__.assert_called_once_with('redditDB')
    db_mock.list_collection_names.assert_called_once()