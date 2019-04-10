import pytest

from unittest.mock import patch, Mock

from speximius.dash_api_utils import (
    resolve_project_key, SHConnection
)


@pytest.mark.parametrize(
    'environ, project_key, expected', [
        ({'SHUB_JOB_DATA': '{"project": "666"}'} , None, '666'),
        ({'SHUB_JOB_DATA': '{"project": "666"}'} , '999', '666'),
        ({'SHUB_JOB_DATA': None} , '999', '999')
    ]
)
def test_resolve_project_key(environ, project_key, expected):
    with patch('os.environ', environ):
        res = resolve_project_key(default_project_key=project_key)
        assert res == expected

def test_resolve_project_key_errors():
    with pytest.raises(NameError):
        res = resolve_project_key()

def test_sh_connection_context():
    client_mock = Mock()
    def client_class_mock(*args):
        return client_mock

    with patch('speximius.dash_api_utils.ScrapinghubClient', client_class_mock):
        with SHConnection(
            'MOCK_API_KEY', default_project_key='666'
        ) as sh_conn:
            project = sh_conn.project

    client_mock.get_project.assert_called_once()
    client_mock.close.assert_called_once()
