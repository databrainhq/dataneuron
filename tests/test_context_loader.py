import pytest
from data_neuron.context_loader import load_context


@pytest.fixture
def mock_file_system(mocker):
    mock_open = mocker.mock_open(
        read_data="table_name: users\ncolumns:\n  - name: id\n    type: INTEGER")
    mocker.patch('builtins.open', mock_open)
    mocker.patch('os.listdir', return_value=['users.yaml'])
    mocker.patch('os.path.join', lambda *args: '/'.join(args))


def test_load_context(mock_file_system):
    context = load_context()
    assert 'tables' in context
    assert 'relationships' in context
    assert 'global_definitions' in context
    assert 'users' in context['tables']
    assert context['tables']['users']['table_name'] == 'users'
