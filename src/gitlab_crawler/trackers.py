import time

from typing import Optional, TypedDict

from bisect import insort

from gitlab_crawler.types_formats import *


class CrawlProcessingTimer:
    """
    Timer used to measure the processing time of recovering all the projects and events.
    Timer launched after the crawler initialization (i.e., after all the backlog projects and events are recovered)
    """

    def __init__(self):
        self._timer_start: Optional[time] = None
        self._elapsed_time: Optional[time] = None
        self._first_processing_measured = False


    def start_timer(self):
        self._timer_start = time.time()


    def stop_timer(self):
        timer_end = time.time()
        self._elapsed_time = timer_end - self._timer_start

        if not self._first_processing_measured:
            self._first_processing_measured = True

        self._timer_start = None


    def get_elapsed_time(self):
        return self._elapsed_time


    def is_timer_started(self):
        return self._timer_start is not None


    def is_first_processing_measured(self):
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

    def __init__(self):
        self._nb_backlog_projects: int = 0
        self._nb_backlog_projects_total: int = 0
        self._backlog_events_recovered: bool = False
        self._nb_backlog_events: int = 0


    def set_nb_projects(self, nb_projects: int):
        self._nb_backlog_projects = nb_projects
        self._nb_backlog_projects_total = nb_projects


    def decr_nb_projects(self):
        self._nb_backlog_projects -= 1


    def get_nb_backlog_projects(self):
        return self._nb_backlog_projects


    def are_all_projects_processed(self):
        return self._nb_backlog_projects == 0


    def incr_nb_events(self, nb_events: int):
        self._nb_backlog_events += nb_events


    def get_nb_events(self):
        return self._nb_backlog_events


    def set_recovery_finished(self):
        self._backlog_events_recovered = True


    def is_recovery_finished(self):
        return self._backlog_events_recovered


class HourlyEvents(TypedDict):
    fetched_new_events: bool
    events: list[GitLabEvent]


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
        self._recent_projects_ids: list[Project_ID] = []
        self._known_events: set[Event_ID] = set()
        self._timestamp_events: Dict[Timestamp, HourlyEvents] = {}
        self._project_list_exhausted: bool = False


    def add_projects(self, projects: list[GitLabProject]):
        for project in projects:
            self._recent_projects_ids.append(object_id(project))


    def pop_project_id(self):
        return self._recent_projects_ids.pop(0)


    def get_nb_projects(self):
        return len(self._recent_projects_ids)


    def is_project_list_exhausted(self):
        return len(self._recent_projects_ids) == 0


    def _add_event_known(self, event_id: int):
        self._known_events.add(event_id)


    def is_event_known(self, event_id: int):
        return event_id in self._known_events


    def get_known_timestamps(self):
        return list(self._timestamp_events.keys())


    def store_event(self, event: GitLabEvent, project_id: int):
        """
        Store the event in the according timestamp (date and hour) in chronological order
        """
        event_id = object_id(event)
        self._add_event_known(event_id)

        event_timestamp = event_creation_date(event).strftime(JSON_DATE_FORMAT)

        if event_timestamp not in self._timestamp_events:
            new_entry: HourlyEvents = {'fetched_new_events': True, 'events': []}
            self._timestamp_events.update({event_timestamp: new_entry})

        target_slot = self._timestamp_events[event_timestamp]
        insort(target_slot['events'], event, key=event_creation_date)

        target_slot['fetched_new_events'] = True


    def are_new_events_found(self):
        known_timestamps = self.get_known_timestamps()

        if len(known_timestamps) == 0:
            return False

        else:
            for timestamp in known_timestamps:
                if self.is_timestamp_updated(timestamp):
                    return True

        return False


    def is_timestamp_updated(self, timestamp: Timestamp):
        return self._timestamp_events[timestamp]['fetched_new_events']


    def get_hourly_events(self, timestamp: Timestamp):
        return self._timestamp_events[timestamp]['events']


    def set_no_new_events(self, timestamp: Timestamp):
        self._timestamp_events[timestamp]['fetched_new_events'] = False


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

    def __init__(self):
        self._nb_recovered_projects: int = 0
        self._projects_recovery_time: Optional[float] = None
        self._projects_response_time: list[float] = []
        self._nb_recovered_events: int = 0
        self._events_response_time: list[float] = []
        self._nb_request_errors: int = 0


    def increase_nb_recovered_projects(self, nb_projects: int):
        self._nb_recovered_projects += nb_projects


    def get_nb_recovered_projects(self):
        return self._nb_recovered_projects


    def set_projects_response_time(self, response_time: list[float]):
        self._projects_response_time = response_time


    def get_projects_response_time(self):
        return self._projects_response_time


    def set_projects_recovery_time(self, recovery_time: float):
        self._projects_recovery_time = recovery_time


    def get_projects_recovery_time(self):
        return self._projects_recovery_time


    def increase_nb_recovered_events(self, nb_events: int):
        self._nb_recovered_events += nb_events


    def get_nb_recovered_events(self):
        return self._nb_recovered_events


    def extend_events_response_time(self, response_time: list[float]):
        self._events_response_time.extend(response_time)


    def get_events_response_time(self):
        return self._events_response_time


    def increment_nb_request_errors(self):
        self._nb_request_errors += 1


    def get_nb_request_errors(self):
        return self._nb_request_errors


    def reset_tracker(self):
        self._nb_recovered_projects = 0
        self._projects_response_time = []
        self._projects_recovery_time = None
        self._nb_recovered_events = 0
        self._events_response_time = []
        self._nb_request_errors = 0