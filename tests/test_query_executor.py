import pytest
from data_neuron.query_executor import execute_query


def test_execute_query(mocker):
    # Mock the sqlite3 connection and cursor
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(10,)]

    mocker.patch('sqlite3.connect', return_value=mock_connection)

    result = execute_query("SELECT COUNT(*) FROM users")
    assert result == "(10,)"
    mock_cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM users")


def test_execute_query_error(mocker):
    # Mock the sqlite3 connection to raise an error
    mock_connection = mocker.Mock()
    mocker.patch('sqlite3.connect', return_value=mock_connection)
    mock_connection.cursor.side_effect = Exception("Database error")

    result = execute_query("SELECT COUNT(*) FROM users")
    assert "An error occurred:" in result
