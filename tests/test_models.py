import copy
from datetime import datetime

import pytest
from unittest.mock import patch, mock_open

from gitlab_crawler.models import ActivityStats
# import gitlab_crawler.models.pd.DataFrame.to_csv

@pytest.fixture
def init_stats() -> ActivityStats:
    """Initialization parameters for ActivityStats objects."""
    return ActivityStats('GitLab', 5)


@pytest.fixture
def csv_data() -> dict:
    return dict(current_time=datetime(2020, 1, 2, 3), nb_projects=2, projects_avg_time=0.5,
                nb_events=2, events_avg_time=0.5, process_time=1.5, nb_errors=2)


def test_init_sets_defaults(init_stats):
    """Test default values after initialization."""
    stats = init_stats

    assert isinstance(stats.data, dict)
    assert 'crawler_config' in stats.data
    assert isinstance(stats.data['crawler_config'], dict)

    for key in ('backlog_projects_recovery', 'backlog_events_recovery', 'event_disk_writes', 'crawls'):
        assert key in stats.data
        assert isinstance(stats.data[key], dict)

    assert stats.json_file_path is None
    assert stats.csv_file_path is None
    assert isinstance(stats.trigger_projects_recovery, dict)
    assert isinstance(stats.trigger_events_recovery, dict)
    assert isinstance(stats.csv_stats, list)
    assert isinstance(stats.hourly_event_writes, dict)


def test_set_file_path_updates_paths(tmp_path, init_stats):
    """Test updating file paths."""
    stats = init_stats
    json_path, csv_path = tmp_path/'output.json', tmp_path/'output.csv'
    stats.set_file_path(json_path=str(json_path), csv_path=str(csv_path))
    assert stats.json_file_path == str(json_path)
    assert stats.csv_file_path == str(csv_path)


# Each tuple in the list below defines:
#   - the name of the setter to test (as a string)
#   - the arguments that should be passed to that setter
#   - the name of the targeted `stats.data` field
@pytest.mark.parametrize(
    'setter,args, target_attr',
    [
        (
            'set_backlog_projects',
            dict(time_window='1h', nb_projects=2, recovery_time=1.5, list_response_time=[0.1, 0.2]),
            'backlog_projects_recovery',
        ),
        (
            'set_backlog_events',
            dict(time_elapsed='1h', nb_events=2, response_time=[0.1, 0.2]),
            'backlog_events_recovery',
        ),
        (
            'set_overall_disk_write',
            dict(current_timestamp=datetime(2020, 1, 2, 3), write_duration=0.5),
            'event_disk_writes',
        ),
        (
            'set_trigger_stats',
            dict(current_time=datetime(2020, 1, 2, 3), processing_time=0.5, nb_errors=1),
            'crawls'
        ),
    ],
)
def test_stats_data_setters(init_stats, setter: str, args: dict, target_attr: str):
    """
    Generic test verifying that each setter modifies only its own section
    of `stats.data` and leaves all unrelated sections untouched.
    """
    stats = init_stats
    before = copy.deepcopy(stats.data)

    # Dynamically call the setter by its string name
    getattr(stats, setter)(**args)

    for key, value in before.items():
        # Check that unrelated fields were not modified
        if key != target_attr:
            assert stats.data[key] == value  # type: ignore[literal-required]

        # Check that target field was modified
        else:
            assert stats.data[key] != value  # type: ignore[literal-required]



# Each tuple in the list below defines:
#   - the name of the setter to test (as a string)
#   - the arguments that should be passed to that setter
#   - the name of the targeted `stats` attribute
@pytest.mark.parametrize(
    'setter,args,target_attr',
    [
        (
            'add_disk_write_entry',
            dict(timestamp='2020-1-2-3', nb_events=2, write_duration=0.1),
            'hourly_event_writes',
        ),
        (
            'set_trigger_projects',
            dict(nb_projects=2, recovery_time=1.5, list_response_time=[0.1, 0.2]),
            'trigger_projects_recovery',
        ),
        (
            'set_trigger_events',
            dict(nb_events=2, list_response_time=[0.1, 0.2]),
            'trigger_events_recovery',
        ),
        (
            'append_csv_stats',
            dict(current_time=datetime(2020, 1, 2, 3), nb_projects=2, projects_avg_time=0.5,
                nb_events=2, events_avg_time=0.5, processing_time=1.5, nb_errors=2),
            'csv_stats',
        ),
    ],
)
def test_stats_attribute_setters(init_stats, setter: str, args: dict, target_attr: str):
    """
    Generic test verifying that each setter modifies the target class attribute.
    """
    stats = init_stats

    # Make a copy of only the targeted attribute
    before = copy.deepcopy(getattr(stats, target_attr))

    getattr(stats, setter)(**args)

    # Compare the updated field with previous state
    after = getattr(stats, target_attr)
    assert before != after


@patch('builtins.open', new_callable=mock_open)
def test_disk_write_json_file(mock_file, tmp_path, init_stats):
    """Test writing the json file."""
    stats = init_stats
    stats.set_file_path(json_path=str(tmp_path/'output.json'), csv_path='')

    stats.disk_write_json()
    mock_file.assert_called_once_with(stats.json_file_path, 'wb')


@patch('pandas.DataFrame.to_csv')
def test_disk_write_csv_file(mock_to_csv, tmp_path, init_stats):
    """Test writing the csv file."""
    stats = init_stats
    stats.set_file_path(json_path='', csv_path=str(tmp_path/'output.csv'))
    stats.append_csv_stats(current_time=datetime(2020, 1, 2, 3), nb_projects=2,
                           projects_avg_time=0.5, nb_events=2, events_avg_time=0.5, processing_time=1.5, nb_errors=2)

    stats.disk_write_csv()

    mock_to_csv.assert_called_once()


def _iter_class_dicts(obj):
    """Iterate over all dict attributes of a class instance."""
    for name, value in vars(obj).items():
        if isinstance(value, dict):
            yield name, value


def _iter_numbers(obj):
    """Recursively yield all numeric values from nested dicts/lists."""
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _iter_numbers(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _iter_numbers(v)
    elif isinstance(obj, (int, float)):
        yield obj


@patch('pandas.DataFrame.to_csv')
@patch ('builtins.open', new_callable=mock_open)
def test_activity_stats_integration(mock_file, mock_to_csv, tmp_path, init_stats):
    # Initialize object
    stats = init_stats
    stats.set_file_path(
        json_path=str(tmp_path/'output.json'),
        csv_path=str(tmp_path/'output.csv')
    )

    # Simulate recovery and tracking steps
    stats.set_backlog_projects(
        time_window='1h',
        nb_projects=3,
        recovery_time=2.5,
        list_response_time=[0.1, 0.2, 0.3]
    )
    stats.set_trigger_events(
        nb_events=2,
        list_response_time=[0.1, 0.2]
    )

    timestamp = '2020-1-2-3'
    stats.add_disk_write_entry(timestamp=timestamp, nb_events=2, write_duration=0.1)
    stats.set_overall_disk_write(current_timestamp=timestamp, write_duration=0.1)

    dt = datetime(2020, 1, 2, 3)

    stats.set_trigger_stats(
        current_time=dt,
        processing_time=2.5,
        nb_errors=1
    )

    # Trigger disk write (mocked)
    stats.disk_write_json()

    stats.append_csv_stats(
        current_time=dt,
        nb_projects=2,
        projects_avg_time=0.5,
        nb_events=2,
        events_avg_time=0.5,
        processing_time=1.5,
        nb_errors=2
    )
    stats.disk_write_csv()

    # Assertions - ensure data flowed correctly
    # File write attempted once per file format
    mock_file.assert_called_once_with(stats.json_file_path, 'wb')
    mock_to_csv.assert_called_once()

    # Ensure top-level data structure consistency
    data = stats.data
    assert 'crawler_config' in data
    assert 'backlog_projects_recovery' in data
    assert isinstance(data['event_disk_writes'], dict)

    # Check: all recovered numeric values are non-negative
    for attr_name, attr_value in _iter_class_dicts(stats):
        for val in _iter_numbers(attr_value):
            assert val >= 0, f'Negative value found in: {attr_name}'
