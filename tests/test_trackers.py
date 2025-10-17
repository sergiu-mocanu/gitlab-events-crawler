from datetime import datetime
from typing import List

import pytest

from gitlab_crawler.trackers import CrawlProcessingTimer, BacklogTracker, ProjectsEventsTracker, TriggerCrawlTracker
from gitlab_crawler.types_formats import GitLabProject, GitLabEvent, object_id, event_creation_date, JSON_DATE_FORMAT


@pytest.fixture
def gitlab_projects() -> List[GitLabProject]:
    res = []

    for i in range (5):
        gl_project = {'id': i}
        res.append(gl_project)

    return res


@pytest.fixture
def gitlab_events() -> List[GitLabEvent]:
    """Return a list of GitLab events with unique timestamps."""
    res = []

    for i in range(5):
        event_id = i
        event_timestamp = datetime(2020, 1, 2, i)
        gl_event = {'id': event_id, 'created_at': str(event_timestamp)}
        res.append(gl_event)

    return res


# --- Test for CrawlProcessingTimer ---

def test_crawl_processing_timer():
    """Verify full lifecycle behavior of CrawlProcessingTimer (start, stop, and state flags)."""
    timer = CrawlProcessingTimer()
    assert not timer.is_timer_started()
    assert not timer.is_first_processing_measured()

    timer.start_timer()
    assert timer.is_timer_started()

    timer.stop_timer()
    assert not timer.is_timer_started()
    assert timer.is_first_processing_measured()
    elapsed_time = timer.get_elapsed_time()
    assert elapsed_time > 0
    assert isinstance(elapsed_time, float)


# --- Test for BacklogTracker ---

def test_backlog_tracker_projects():
    """Verify that backlog project recovery is well tracked."""
    tracker = BacklogTracker()
    assert tracker.get_nb_backlog_projects() == 0

    tracker.set_nb_projects(5)
    assert tracker.get_nb_backlog_projects() == 5

    for i in reversed(range(5)):
        tracker.decr_nb_projects()
        assert tracker.get_nb_backlog_projects() == i

    assert tracker.are_all_projects_processed()


def test_backlog_tracker_events():
    """Verify that backlog event recovery is well tracked."""
    tracker = BacklogTracker()
    assert tracker.get_nb_events() == 0
    assert not tracker.is_recovery_finished()

    tracker.incr_nb_events(5)
    assert tracker.get_nb_events() == 5

    tracker.set_recovery_finished()
    assert tracker.is_recovery_finished()


# --- Test for ProjectsEventsTracker ---

def test_project_tracker(gitlab_projects):
    """Verify that project recovery is well tracked."""
    tracker = ProjectsEventsTracker()
    assert tracker.get_nb_projects() == 0

    tracker.add_projects(gitlab_projects)
    assert tracker.get_nb_projects() == len(gitlab_projects)
    assert not tracker.is_project_list_exhausted()

    first_project_id = gitlab_projects[0]['id']
    first_stored_project_id = tracker.pop_project_id()
    assert first_project_id == first_stored_project_id

    nb_remaining_projects = tracker.get_nb_projects()
    for _ in range(nb_remaining_projects):
        assert not tracker.is_project_list_exhausted()
        tracker.pop_project_id()
    assert tracker.is_project_list_exhausted()


def test_event_tracker_add_one(gitlab_events):
    """Verify that event recovery is well tracked (one addition)."""
    tracker = ProjectsEventsTracker()
    assert not tracker.are_new_events_found()

    first_event = gitlab_events[0]
    event_id = object_id(first_event)
    event_date = event_creation_date(first_event).strftime(JSON_DATE_FORMAT)

    assert not tracker.is_event_known(event_id)

    tracker.store_event(first_event, 0)
    assert tracker.are_new_events_found()
    assert tracker.is_event_known(event_id)

    known_timestamps = tracker.get_known_timestamps()
    assert event_date in known_timestamps
    assert len(known_timestamps) == 1
    assert tracker.is_timestamp_updated(event_date)


def test_event_tracker_add_multiple(gitlab_events):
    """Verify that event recovery is well tracked (multiple additions)."""
    tracker = ProjectsEventsTracker()

    event_timestamps = []
    for event in gitlab_events:
        tracker.store_event(event, 0)
        event_timestamps.append(event_creation_date(event).strftime(JSON_DATE_FORMAT))

    assert len(tracker.get_known_timestamps()) == len(event_timestamps)
    for timestamp in event_timestamps:
        assert tracker.is_timestamp_updated(timestamp)
        assert len(tracker.get_hourly_events(timestamp)) == 1
        tracker.set_no_new_events(timestamp)

    assert not tracker.are_new_events_found()


def test_event_tracker_add_same_timestamp(gitlab_events):
    """Verify that event tracker recovery is well tracked for multiple similar timestamps."""
    tracker = ProjectsEventsTracker()

    first_event = gitlab_events[0]
    event_timestamp = event_creation_date(first_event).strftime(JSON_DATE_FORMAT)

    nb_additions = 5
    for i in range(nb_additions):
        tracker.store_event(first_event, i)

    assert len(tracker.get_known_timestamps()) == 1
    assert len(tracker.get_hourly_events(event_timestamp)) == nb_additions


# --- Test for TriggerCrawlTracker ---

def test_single_crawl_project_tracker():
    """Verify that single-crawl project recovery is well tracked."""
    crawl_tracker = TriggerCrawlTracker()
    assert crawl_tracker.get_nb_recovered_projects() == 0

    crawl_tracker.increase_nb_recovered_projects(5)
    assert crawl_tracker.get_nb_recovered_projects() == 5

    nb_requests = 5
    server_response_time = []
    for i in range(nb_requests):
        server_response_time.append(i/10)

    crawl_tracker.set_projects_response_time(server_response_time)
    crawl_response_time = crawl_tracker.get_projects_response_time()
    assert len(crawl_response_time) == nb_requests
    assert crawl_response_time == server_response_time

    projects_recovery_time = 1.0
    crawl_tracker.set_projects_recovery_time(projects_recovery_time)
    assert crawl_tracker.get_projects_recovery_time() == projects_recovery_time


def test_single_crawl_event_tracker():
    """Verify that single-crawl event recovery is well tracked."""
    crawl_tracker = TriggerCrawlTracker()
    assert crawl_tracker.get_nb_recovered_events() == 0

    crawl_tracker.increase_nb_recovered_events(5)
    assert crawl_tracker.get_nb_recovered_events() == 5

    first_nb_requests = 5
    first_server_response_time = []
    for i in range(first_nb_requests):
        first_server_response_time.append(i / 10)

    crawl_tracker.extend_events_response_time(first_server_response_time)
    crawl_response_time = crawl_tracker.get_events_response_time()
    assert len(crawl_response_time) == first_nb_requests
    assert crawl_response_time == first_server_response_time

    second_nb_requests = 3
    second_server_response_time = []
    for i in range(first_nb_requests, first_nb_requests + second_nb_requests):
        second_server_response_time.append(i / 10)

    crawl_tracker.extend_events_response_time(second_server_response_time)
    entire_crawl_response_time = crawl_tracker.get_events_response_time()

    second_crawl_response_time = entire_crawl_response_time[-second_nb_requests:]
    assert second_crawl_response_time == second_server_response_time
    assert len(entire_crawl_response_time) == first_nb_requests + second_nb_requests












