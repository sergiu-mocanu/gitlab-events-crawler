import pytest
from datetime import datetime, timezone
from pathlib import Path

from gitlab_crawler.utils import (
    find_project_root, total_and_avg_time, remove_milliseconds,
    reset_hour_beginning, datetime_to_str, datetime_to_hour
)


@pytest.fixture
def marker_name() -> str:
    """Marker used in `find_project_root` tets."""
    return 'requirements.txt'


# --- Tests for find_project_root ---

def test_find_project_root_returns_current_dir(monkeypatch, tmp_path, marker_name):
    """Should return current dir if marker exists there."""
    (tmp_path / marker_name).touch()

    # Pretend that cwd is tmp_path
    monkeypatch.chdir(tmp_path)
    result = find_project_root()

    assert result == tmp_path
    assert isinstance(result, Path)


def test_find_project_root_returns_parent_dir(monkeypatch, tmp_path, marker_name):
    """Should climb up and find marker in parent."""
    parent = tmp_path / 'project'
    child = parent / 'subdir'
    child.mkdir(parents=True)
    (parent / marker_name).touch()

    # Simulate running inside nested subdir
    monkeypatch.chdir(child)
    result = find_project_root()
    assert result == parent


def test_find_project_root_multiple_matches(monkeypatch, tmp_path, marker_name):
    """Should return the nearest match (current dir first)."""
    parent = tmp_path / 'outer'
    child = parent / 'inner'
    child.mkdir(parents=True)
    (parent / marker_name).touch()
    (child / marker_name).touch()

    monkeypatch.chdir(child)
    result = find_project_root()
    assert result == child


def test_find_project_root_not_found(monkeypatch, tmp_path, marker_name):
    """Should raise FileNotFoundError if marker is missing."""
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError):
        find_project_root()


def test_find_project_root_custom_marker(monkeypatch, tmp_path):
    """Should return current dir if custom marker exists there."""
    custom_marker = 'pyproject.toml'
    (tmp_path / custom_marker).touch()
    monkeypatch.chdir(tmp_path)

    result = find_project_root(custom_marker)
    assert result == tmp_path


# --- Tests for total_and_avg_time ---

def test_total_and_avg_time_normal_case():
    """Should return total and avg time for normal case."""
    list_time = [1.0, 2.0, 3.0]

    (total, avg) = total_and_avg_time(list_time)
    assert total == 6.0
    assert avg == 2.0
    assert isinstance(total, float)
    assert isinstance(avg, float)


def test_total_and_avg_time_empty_list():
    """Should return null total and avg time for empty list."""
    list_time = []

    (total, avg) = total_and_avg_time(list_time)
    assert total == 0.0
    assert avg == 0.0


def test_total_and_avg_time_single_value():
    """Should return total and avg time for single value."""
    time = 3.0
    list_time = [time]

    (total, avg) = total_and_avg_time(list_time)
    assert total == time
    assert avg == time


def test_total_and_avg_time_precision():
    """Should test the floating point precision."""
    list_time = [0.1, 0.2, 0.3]

    (total, avg) = total_and_avg_time(list_time)
    assert total == pytest.approx(0.6, rel=1e-9)
    assert avg == pytest.approx(0.2, rel=1e-9)


# --- Test for remove_milliseconds ---

def test_remove_milliseconds():
    """Should remove milliseconds from datetime string."""
    dt = datetime(2020, 1, 2, 3, 4, 5, 123456)
    res = remove_milliseconds(dt)
    assert isinstance(res, datetime)
    assert res.microsecond == 0
    assert res.year == 2020


# --- Test for reset_hour_beginning ---

def test_reset_hour_beginning():
    """Should reset datetime to the beginning of the hour."""
    dt = datetime(2020, 1, 2, 3, 4, 5, 123456)
    res = reset_hour_beginning(dt)
    assert isinstance(res, datetime)
    assert res.microsecond == 0
    assert res.second == 0
    assert res.minute == 0
    assert res.hour == 3


# --- Tests for datetime_to_str ---

def test_datetime_to_str():
    """Should convert datetime to string."""
    dt = datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    res = datetime_to_str(dt)
    assert isinstance(res, str)
    assert res == '2020-01-02T03:04:05'


# --- Tests for datetime_to_hour ---

def test_datetime_to_hour():
    """Should convert datetime to string."""
    dt = datetime(2020, 1, 2, 3)
    res = datetime_to_hour(dt)
    assert isinstance(res, str)
    assert res == '2020-01-02--3'
