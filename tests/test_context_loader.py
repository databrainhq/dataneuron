import unittest
from unittest.mock import patch, mock_open
from data_neuron.core.context_loader import load_context


class TestContextLoader(unittest.TestCase):
    @patch('builtins.open', new_callable=mock_open, read_data="table_name: users\ncolumns:\n  - name: id\n    type: INTEGER")
    @patch('os.listdir')
    @patch('os.path.join')
    def test_load_context(self, mock_join, mock_listdir, mock_file):
        mock_listdir.return_value = ['users.yaml']
        mock_join.side_effect = lambda *args: '/'.join(args)

        context = load_context()
        self.assertIn('tables', context)
        self.assertIn('relationships', context)
        self.assertIn('global_definitions', context)
        self.assertIn('users', context['tables'])
        self.assertEqual(context['tables']['users']['table_name'], 'users')


if __name__ == '__main__':
    unittest.main()
