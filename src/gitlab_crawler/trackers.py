import time
from datetime import datetime

from typing import Optional

from bisect import insort

from dataclasses import dataclass

from gitlab_crawler.types_formats import *


class CrawlProcessingTimer:
    """
    Timer used to measure the processing time of recovering all the projects and events.
    Timer launched after the crawler initialization (i.e., after all the backlog projects and events are recovered).
    """

    def __init__(self) -> None:
        self._timer_start: Optional[time] = None
        self._elapsed_time: Optional[time] = None
        self._first_processing_measured = False


    def start_timer(self) -> None:
        self._timer_start = time.time()


    def stop_timer(self) -> None:
        timer_end = time.time()
        self._elapsed_time = timer_end - self._timer_start

        if not self._first_processing_measured:
            self._first_processing_measured = True

        self._timer_start = None


    def get_elapsed_time(self) -> Optional[time]:
        return self._elapsed_time


    def is_timer_started(self) -> bool:
        return self._timer_start is not None


    def is_first_processing_measured(self) -> bool:
        return self._first_processing_measured


class BacklogTracker:
    """
    Track the number of 'backlog' projects and events recovered during the crawler initialization

    'Backlog' projects:
    Due to the behavior of the GitLab API, the 'last_activity_at' field of a project (which is used to filter the
    projects with recent activity) is updated at most once per 1h. Therefore, in order to guarantee the recovery of all
    the events created during the crawler start hour, it is necessary to recover all the projects that have been updated
    since the beginning of the previous hour (i.e., if the crawler is launched at 13h30, it will look for projects
    updated since 12h00): called backlog projects.

    'Backlog' events:
    In the context of the example from above, the 'backlog' events are events that were created since the start of the
    hour of the crawler launch (from 13h00 to 13h30). However, due to the potentially considerable amount of projects
    updated since 12h00, the 'backlog' event recovery can take up to 20 minutes.
    """

    def __init__(self) -> None:
        self._nb_backlog_projects: int = 0
        self._nb_backlog_projects_total: int = 0
        self._backlog_events_recovered: bool = False
        self._nb_backlog_events: int = 0


    def set_nb_projects(self, nb_projects: int) -> None:
        self._nb_backlog_projects = nb_projects
        self._nb_backlog_projects_total = nb_projects


    def decr_nb_projects(self) -> None:
        self._nb_backlog_projects -= 1


    def get_nb_backlog_projects(self) -> int:
        return self._nb_backlog_projects


    def are_all_projects_processed(self) -> bool:
        return self._nb_backlog_projects == 0


    def incr_nb_events(self, nb_events: int) -> None:
        self._nb_backlog_events += nb_events


    def get_nb_events(self) -> int:
        return self._nb_backlog_events


    def set_recovery_finished(self) -> None:
        self._backlog_events_recovered = True


    def is_recovery_finished(self) -> bool:
        return self._backlog_events_recovered


@dataclass
class LatestEvent:
    event_id: Event_ID
    created_at: datetime


class ProjectsEventsTracker:
    """
    Store and manage the recovered projects and events (sorted by hour)

    Attributes:
          _recent_projects_ids: recovered projects that haven't been treated (no events recovered)
          _known_events: list of events id (and the corresponding project id) that have been stored
          _timestamp_events: dictionary that sorts the recovered events by date and hour (in chronological, ascending order)
          _project_list_exhausted: boolean that indicates if all the known projects have been treated (events recovered)
    """

    def __init__(self):
        self._updated_projects: dict[Project_ID, None] = {}
        self._previously_updated_projects: dict[Project_ID, None] = {}
        self._latest_events: Dict[Project_ID, LatestEvent] = {}
        self._events_payload: Dict[Timestamp, list[GitLabEvent]] = {}
        self._project_list_exhausted: bool = False


    def add_projects(self, projects: list[GitLabProject]) -> None:
        for project in projects:
            project_id = object_id(project)
            self._updated_projects[project_id] = None


    def pop_project_id(self) -> int:
        project_id, _ = self._updated_projects.popitem()
        return project_id


    def get_nb_projects(self) -> int:
        return len(self._updated_projects)


    def is_project_list_exhausted(self) -> bool:
        return len(self._updated_projects) == 0


    def get_project_last_update(self, project_id: Project_ID) -> Optional[datetime]:
        """
        Return the datetime of the last updated event for the given project.
        Return None if project wasn't previously recovered.
        """
        if project_id in self._latest_events:
            return self._latest_events[project_id].created_at
        else:
            return None


    def get_updated_timestamps(self) -> list[Timestamp]:
        """
        Return the list of timestamps that contain newly-recovered events.
        """
        return list(self._events_payload.keys())


    def store_events(self, project_id: Project_ID, events: list[GitLabEvent]):
        """
        Store the events in the according timestamp (date and hour) in a chronological order.
        """
        if len(events) != 0:
            most_recent_event: GitLabEvent = events[0]
            event_id: int = object_id(most_recent_event)
            created_at: datetime = event_creation_date(most_recent_event)
            self._latest_events[project_id] = LatestEvent(event_id, created_at)

            for event in events:
                formatted_timestamp: str = event_creation_date(event).strftime(JSON_DATE_FORMAT)

                if formatted_timestamp not in self._events_payload:
                    self._events_payload[formatted_timestamp] = []

                target_slot: list[GitLabEvent] = self._events_payload[formatted_timestamp]
                insort(target_slot, event, key=event_creation_date)


    def are_new_events_found(self) -> bool:
        return bool(self._events_payload)


    def get_hourly_events(self, timestamp: Timestamp) -> list[GitLabEvent]:
        return self._events_payload[timestamp]


    def reset_new_events(self) -> None:
        self._events_payload = {}


class TriggerCrawlTracker:
    """
    Track the number of projects and events recovered between two crawls

    Attributes:
        _nb_recovered_projects: number of recovered projects
        _projects_recovery_time: total duration of project recovery (parallelized recovery)
        _projects_response_time: list of all the projects request response time
        _nb_recovered_events: number of recovered events
        _events_response_time: list of all the events request response time
        _nb_request_errors: number of failed requests (e.g., timeout, connection error)
    """

    def __init__(self) -> None:
        self._nb_recovered_projects: int = 0
        self._projects_recovery_time: Optional[float] = None
        self._projects_response_time: list[float] = []
        self._nb_recovered_events: int = 0
        self._events_response_time: list[float] = []
        self._nb_request_errors: int = 0


    def increase_nb_recovered_projects(self, nb_projects: int) -> None:
        self._nb_recovered_projects += nb_projects


    def get_nb_recovered_projects(self) -> int:
        return self._nb_recovered_projects


    def set_projects_response_time(self, response_time: list[float]) -> None:
        self._projects_response_time = response_time


    def get_projects_response_time(self) -> list[float]:
        return self._projects_response_time


    def set_projects_recovery_time(self, recovery_time: float) -> None:
        self._projects_recovery_time = recovery_time


    def get_projects_recovery_time(self) -> float:
        return self._projects_recovery_time


    def increase_nb_recovered_events(self, nb_events: int) -> None:
        self._nb_recovered_events += nb_events


    def get_nb_recovered_events(self) -> int:
        return self._nb_recovered_events


    def extend_events_response_time(self, response_time: list[float]) -> None:
        self._events_response_time.extend(response_time)


    def get_events_response_time(self) -> list[float]:
        return self._events_response_time


    def increment_nb_request_errors(self) -> None:
        self._nb_request_errors += 1


    def get_nb_request_errors(self) -> int:
        return self._nb_request_errors


    def reset_tracker(self) -> None:
        self._nb_recovered_projects = 0
        self._projects_response_time = []
        self._projects_recovery_time = None
        self._nb_recovered_events = 0
        self._events_response_time = []
        self._nb_request_errors = 0