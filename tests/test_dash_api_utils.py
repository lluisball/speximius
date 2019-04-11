import pytest

from unittest.mock import patch, Mock

from speximius import dash_api_utils


@pytest.mark.parametrize(
    'environ, project_key, expected', [
        ({'SHUB_JOB_DATA': '{"project": "666"}'}, None, '666'),
        ({'SHUB_JOB_DATA': '{"project": "666"}'}, '999', '666'),
        ({'SHUB_JOB_DATA': None}, '999', '999')
    ]
)
def test_resolve_project_key(environ, project_key, expected):
    with patch('os.environ', environ):
        res = dash_api_utils.resolve_project_key(
            default_project_key=project_key
        )
        assert res == expected


def test_resolve_project_key_errors():
    with pytest.raises(NameError):
        dash_api_utils.resolve_project_key()


def test_sh_connection_context():
    client_mock = Mock()

    def client_class_mock(*args):
        return client_mock

    with patch('speximius.dash_api_utils.ScrapinghubClient', client_class_mock):
        with dash_api_utils.SHConnection(
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

    assert expected == list(dash_api_utils.jobs_iter(mock_connection))


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
        assert list(dash_api_utils.jobs_iter(mock_connection))  == [1, 2, 3, 42]
    assert mock_sleep.call_count == 1


def test_jobs_iter_connect_permanent_failure():
    ''' Simulate an exception during job iteration consumtion,
    in this case the exception does not go away and persists.
    Ensure we do not have an infinite retry
    '''

    def mock_iter(**kwargs):
        yield 1
        yield 42
        raise ValueError()

    mock_connection = Mock()
    mock_connection.jobs_iter = mock_iter

    mock_sleep = Mock()
    with patch('speximius.dash_api_utils.sleep', mock_sleep):
        assert list(dash_api_utils.jobs_iter(mock_connection))  == [1, 42]
    assert mock_sleep.call_count == dash_api_utils.MAX_JOBS_RETRIES


@pytest.mark.parametrize(
    'jobs_per_page, expected', [
        (2, ['a_1', 'b_1', 'a_2', 'b_2', 'a_3', 'b_3', 'a_4']),

        # Having pages bigger than API would simulate getting to last page,
        # this will ensure that we check for last page scenario.
        (20, ['a_1', 'b_1']),
    ]
)
def test_get_jobs(jobs_per_page, expected):

    call_times = iter([1, 2, 3, 4])
    def mock_iter(**kwargs):
        ''' Expected 7 items, 2 for invocations 1,2 and 3
        and 1 for invocation 4.
        Expected is: [
            {'key': 'a_1'}, {'key': 'b_1'},
            {'key': 'a_2'}, {'key': 'b_2'},
            {'key': 'a_3'}, {'key': 'b_3'},
            {'key': 'a_4'}
        ]
        '''
        call_time = next(call_times)
        yield {'key': 'a_{vid}'.format(vid=call_time)}
        if call_time < 4:
            yield {'key': 'b_{vid}'.format(vid=call_time)}

    # Small page size test
    mock_connection = Mock()
    mock_connection.jobs_iter = mock_iter
    with patch('speximius.dash_api_utils.JOBS_PER_PAGE', jobs_per_page):
        jobs = list(dash_api_utils.get_jobs(mock_connection))
        assert jobs == expected
