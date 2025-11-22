import copy

import orjson
import pandas as pd

from typing import Optional, Dict, TypedDict
from datetime import datetime

from gitlab_crawler.utils import datetime_to_str, total_and_avg_time
from gitlab_crawler.types_formats import Timestamp


class GitLabToken:
    """
        GitLab token, the value is the one you get when you create the token on the forge website and is read from the file file_name.
        Attributes:
          file_name (str): file path that contains the token.
          value (str): the token to pass in the request.
    """

    def __init__(self, file_name: str):
        self.file_name: str = file_name
        with open(self.file_name, "r") as token_file:
            token = token_file.read().strip()
            self.value: str = token


    def print_object(self):
        print(f"ForgeToken: file_name = {self.file_name} - value = {self.value}")


class GitLabInstance:
    """
        The GitLab instance for recovering the most recent updated projects' metadata. You shall have created a token before.

        Attributes:
          gl_instance (str): name of the GitLab instance (e.g., gitlab.com, gitlab.softwareheritage.org)
          url (str): endpoint URL for obtaining GitLab projects by recent activity
    """

    def __init__(self, gl_instance: str):
        self.raw_name = gl_instance

        if 'https://' not in gl_instance:
            self.gl_instance = 'https://' + gl_instance + '/'
        else:
            self.gl_instance = gl_instance

        self.url = self.gl_instance + 'api/v4/projects?order_by=last_activity_at'


    def __getattr__(self, name):
        if name == 'name':
            value = self.gl_instance
        elif name == 'raw_name':
            value = self.raw_name
        elif name == 'url':
            value = self.url
        else:
            raise AttributeError(name)
        return value


    def print_object(self):
        print(f'Instance = {self.gl_instance} - url = {self.url}')


"""
Dictionary templates used for the activity stats written to JSON/CSV files
"""
class CrawlerConfigDict(TypedDict):
    instance: str
    trigger_frequency: int


class BacklogProjects(TypedDict):
    backlog_time_window: str
    nb_recovered_projects: int
    recovery_time: float
    response_time_average: float
    response_time_list: list[float]


class BacklogEvents(TypedDict):
    events_recovery_time: str
    nb_recovered_events: int
    avg_response_time: float
    response_time: list[float]


class DiskWriteTimestamp(TypedDict):
    nb_events: int
    write_duration: float


class DiskWriteHourly(TypedDict):
    total_write_duration: float
    hourly_event_writes: Dict[Timestamp, DiskWriteTimestamp]


class TriggerProjectRecovery(TypedDict):
    nb_recovered_projects: int
    projects_recovery_time: float
    projects_response_time_avg: float
    projects_response_time_list: list[float]


class TriggerEventRecovery(TypedDict):
    nb_processed_projects: int
    nb_recovered_events: int
    events_response_time_avg: float
    events_response_time_total: str
    events_response_time_list: list[float]


class TriggerStatsDict(TypedDict):
    project_recovery: TriggerProjectRecovery
    event_recovery: TriggerEventRecovery
    total_processing_time: float
    nb_request_errors: int


# Top-level dict
class OverallStats(TypedDict):
    crawler_config: CrawlerConfigDict
    backlog_projects_recovery: BacklogProjects
    backlog_events_recovery: BacklogEvents
    event_disk_writes: Dict[Timestamp, DiskWriteHourly]
    crawls: Dict[Timestamp, TriggerStatsDict]


class CSVStats(TypedDict):
    datetime: str
    weekday: str
    nb_recovered_projects: int
    projects_avg_response_time: float
    nb_recovered_events: int
    events_avg_response_time: float
    processing_time: float
    nb_request_errors: int


class ActivityStats:
    """
    Manage the format of all the crawling and activity stats written to JSON/CSV files.
    This includes backlog project/event recovery, crawl recovery, server response time, disk writes
    """
    EMPTY_BACKLOG_PROJECTS: BacklogProjects = {
        'backlog_time_window': '',
        'nb_recovered_projects': 0,
        'recovery_time': 0.0,
        'response_time_average': 0.0,
        'response_time_list': []
    }

    EMPTY_BACKLOG_EVENTS: BacklogEvents = {
        'events_recovery_time': '',
        'nb_recovered_events': 0,
        'avg_response_time': 0.0,
        'response_time': []
    }

    EMPTY_TRIGGER_PROJECT_RECOVERY: TriggerProjectRecovery = {
        'nb_recovered_projects': 0,
        'projects_recovery_time': 0.0,
        'projects_response_time_avg': 0.0,
        'projects_response_time_list': []
    }

    EMPTY_TRIGGER_EVENT_RECOVERY: TriggerEventRecovery = {
        'nb_processed_projects': 0,
        'nb_recovered_events': 0,
        'events_response_time_avg': 0.0,
        'events_response_time_total': '',
        'events_response_time_list': []
    }

    def __init__(self, gl_instance: str, trigger_frequency: int):
        self.data: OverallStats = {
            'crawler_config': {'instance': gl_instance, 'trigger_frequency': trigger_frequency},
            'backlog_projects_recovery': self.EMPTY_BACKLOG_PROJECTS.copy(),
            'backlog_events_recovery': self.EMPTY_BACKLOG_EVENTS.copy(),
            'event_disk_writes': {},
            'crawls': {}
        }
        self.trigger_projects_recovery = self.EMPTY_TRIGGER_PROJECT_RECOVERY.copy()
        self.trigger_events_recovery = self.EMPTY_TRIGGER_EVENT_RECOVERY.copy()
        self.csv_stats: list[CSVStats] = []
        self.hourly_event_writes: Dict[Timestamp, DiskWriteTimestamp] = {}
        self.json_file_path: Optional[str] = None
        self.csv_file_path: Optional[str] = None


    def set_file_path(self, *, json_path: str, csv_path: str):
        self.json_file_path = json_path
        self.csv_file_path = csv_path


    def set_backlog_projects(self, *, time_window: str, nb_projects: int,
                             recovery_time: float, list_response_time: list[float]):
        _, avg_response_time = total_and_avg_time(list_response_time)

        self.data['backlog_projects_recovery'] = BacklogProjects(backlog_time_window=time_window,
                                                                 nb_recovered_projects=nb_projects,
                                                                 recovery_time=recovery_time,
                                                                 response_time_average=avg_response_time,
                                                                 response_time_list=list_response_time)


    def set_backlog_events(self, *, time_elapsed: str, nb_events: int, response_time: list[float]):
        _, avg_response_time = total_and_avg_time(response_time)
        self.data['backlog_events_recovery'] = BacklogEvents(events_recovery_time=time_elapsed,
                                                             nb_recovered_events=nb_events,
                                                             avg_response_time=avg_response_time,
                                                             response_time=response_time)


    def add_disk_write_entry(self, timestamp: Timestamp, nb_events: int, write_duration: float):
        disk_write_entry: DiskWriteTimestamp = {'nb_events': nb_events, 'write_duration': write_duration}
        self.hourly_event_writes.update({timestamp: disk_write_entry})


    def set_overall_disk_write(self, current_timestamp: Timestamp, write_duration: float):
        hourly_write_entry = DiskWriteHourly(total_write_duration=write_duration,
                                             hourly_event_writes=copy.deepcopy(self.hourly_event_writes))
        self.data['event_disk_writes'].update({current_timestamp: hourly_write_entry})
        self.hourly_event_writes.clear()


    def set_trigger_projects(self, nb_projects: int, recovery_time: float, list_response_time: list[float]):
        _, avg_response_time = total_and_avg_time(list_response_time)
        self.trigger_projects_recovery = {'nb_recovered_projects': nb_projects,
                                          'projects_recovery_time': recovery_time,
                                          'projects_response_time_avg': avg_response_time,
                                          'projects_response_time_list': list_response_time}


    def set_trigger_events(self, *, nb_events: int, list_response_time: list[float]):
        total_response_time, avg_response_time = total_and_avg_time(list_response_time)
        self.trigger_events_recovery = {'nb_recovered_events': nb_events,
                                        'events_response_time_avg': avg_response_time,
                                        'events_response_time_total': total_response_time,
                                        'events_response_time_list': list_response_time}


    def set_trigger_stats(self, *, current_time: datetime, processing_time: float, nb_errors: int):
        stats_entry: TriggerStatsDict = {'project_recovery': self.trigger_projects_recovery,
                                         'event_recovery': self.trigger_events_recovery,
                                         'total_processing_time': processing_time,
                                         'nb_request_errors': nb_errors}

        timestamp = datetime_to_str(current_time)
        self.data['crawls'][timestamp] = stats_entry


    def append_csv_stats(self, *, current_time: datetime, nb_projects: int, projects_avg_time: float,
                      nb_events: int, events_avg_time: float, processing_time: float, nb_errors: int):

        # Extract the day of the week (e.g., Mon, Tue, Wen)
        weekday = current_time.strftime('%a')
        timestamp = datetime_to_str(current_time)

        csv_entry: CSVStats = {'datetime': timestamp,
                          'weekday': weekday,
                          'nb_recovered_projects': nb_projects,
                          'projects_avg_response_time': projects_avg_time,
                          'nb_recovered_events': nb_events,
                          'events_avg_response_time': events_avg_time,
                          'processing_time': processing_time,
                          'nb_request_errors': nb_errors}

        self.csv_stats.append(csv_entry)


    def disk_write_json(self):
        with open(self.json_file_path, 'wb') as stats_file:
            stats_file.write(orjson.dumps(self.data))


    def disk_write_csv(self):
        stats_df_file = pd.DataFrame.from_records(self.csv_stats)
        stats_df_file.to_csv(self.csv_file_path, index=False)