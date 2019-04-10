import pytest

from unittest.mock import patch, Mock

from speximius.dash_api_utils import (
    resolve_project_key, SHConnection, jobs_iter
)


@pytest.mark.parametrize(
    'environ, project_key, expected', [
        ({'SHUB_JOB_DATA': '{"project": "666"}'}, None, '666'),
        ({'SHUB_JOB_DATA': '{"project": "666"}'}, '999', '666'),
        ({'SHUB_JOB_DATA': None}, '999', '999')
    ]
)
def test_resolve_project_key(environ, project_key, expected):
    with patch('os.environ', environ):
        res = resolve_project_key(default_project_key=project_key)
        assert res == expected


def test_resolve_project_key_errors():
    with pytest.raises(NameError):
        resolve_project_key()


def test_sh_connection_context():
    client_mock = Mock()

    def client_class_mock(*args):
        return client_mock

    with patch('speximius.dash_api_utils.ScrapinghubClient', client_class_mock):
        with SHConnection(
            'MOCK_API_KEY', default_project_key='666'
        ) as sh_conn:
            sh_conn.project

    client_mock.get_project.assert_called_once()
    client_mock.close.assert_called_once()


@pytest.mark.parametrize(
    'jobs, expected', [
        ([1,2,3,4,5], [1,2,3,4,5]),
        ([1,1,2,2,3,4,5], [1,2,3,4,5]),
        ([], [])
    ]
)
def test_jobs_iter(jobs, expected):
    mock_connection = Mock()
    mock_connection.jobs_iter = lambda **kw: jobs

    assert expected == list(jobs_iter(mock_connection))


def test_jobs_iter_connect_issues():
    ''' Simulate an exception during job iteration consumtion,
    ensure that despite of starting over we pause and get the
    rigth result, duplicates free.
    '''

    times = iter([0, 1])
    def mock_iter(**kwargs):
        times_now = next(times)
        if times_now == 1:
            for x in [1, 2, 3, 42]:
                yield x
        elif times_now == 0:
            yield 1
            raise ValueError()

    mock_connection = Mock()
    mock_connection.jobs_iter = mock_iter

    mock_sleep = Mock()
    with patch('speximius.dash_api_utils.sleep', mock_sleep):
        assert list(jobs_iter(mock_connection))  == [1, 2, 3, 42]
    assert mock_sleep.call_count == 1
