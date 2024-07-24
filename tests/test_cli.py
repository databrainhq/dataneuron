import pytest
from click.testing import CliRunner
from data_neuron.cli import main


@pytest.fixture
def runner():
    return CliRunner()


def test_cli_with_valid_input(runner, mocker):
    # Mock the necessary functions
    mocker.patch('data_neuron.cli.load_context', return_value={})
    mocker.patch('data_neuron.cli.process_with_llm',
                 return_value="SELECT COUNT(*) FROM users")
    mocker.patch('data_neuron.cli.execute_query', return_value="10")

    result = runner.invoke(main, ['--ask', 'total users count'])
    assert result.exit_code == 0
    assert 'Query: SELECT COUNT(*) FROM users' in result.output
    assert 'Result:\n10' in result.output


def test_cli_with_invalid_directory(runner, mocker):
    # Mock os.path.exists to simulate missing directory
    mocker.patch('os.path.exists', return_value=False)

    result = runner.invoke(main, ['--ask', 'total users count'])
    assert result.exit_code == 0
    assert "Error: Make sure you're in the correct directory" in result.output
