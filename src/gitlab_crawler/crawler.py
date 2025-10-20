import logging
import asyncio
import signal
import os
import aiohttp
import orjson

from typing import Union, cast

from datetime import timedelta, timezone

from gitlab_crawler.models import GitLabInstance, GitLabToken, ActivityStats
from gitlab_crawler.trackers import *
from gitlab_crawler.types_formats import GitLabEvent, GitLabProject
from gitlab_crawler.utils import *


logging.basicConfig(
            level = logging.INFO,
            format = '[%(process)d:%(levelname)s] %(asctime)s :: %(message)s'
        )
logger = logging.getLogger("gl-swharchive")


class GitLabCrawler:
    """
        Fetch the latest events from the GitLab API, write the events and activity stats to JSON files.

        Attributes:
            config: crawler config
            trigger_tracker: tracker for projects and events recovered during the current crawl
            backlog_tracker: track the backlog projects and events
            activity_stats: track and document the forge activity
            projects_events: manage the recovered projects and events
            processing_timer: measure the time for recovering all the projects and events

        """

    class CrawlerConfig:
        """
        Set the configuration of the crawler

        Args:
            gl_instance (GitLabInstance): The GitLab instance to fetch events from.
            gl_token (str): The GitLab token to access the REST API (used only for 'gitlab.com' instance)
            trigger_frequency (int): frequency of forge crawls (in minutes)
            timeout_value (int): timeout value for consecutive failed requests (in minutes)
            delay (int): delay between two consecutive, failed requests (in seconds)
            verbose (bool): create more detailed logger messages
            data_dir (str): path to the directory where events and activity stats will be stored
        """
        def __init__(self, *, gl_instance: GitLabInstance, trigger_frequency: int, timeout_value: int, delay: int,
                     verbose: bool, data_dir: str, gl_token: GitLabToken = None):
            self.crawler_start_hour: datetime = reset_hour_beginning(datetime.now(timezone.utc))
            self.gl_instance: GitLabInstance = gl_instance
            self.gl_token: GitLabToken = gl_token
            self.trigger_frequency: int = trigger_frequency * 60
            self.timeout_value: int = timeout_value
            self.request_delay: int = delay
            self.verbose: bool = verbose
            self.data_dir: str = data_dir
            self.events_dir: Optional[str] = None
            self.stats_dir: Optional[str] = None
            self.page_limit: int = 20
            self.request_header = {'Content-Type': 'application/json'}
            if gl_token is not None:
                self.request_header.update({'Authorization': f'Bearer {self.gl_token.value}'})

    def __init__(self, config: CrawlerConfig):
        self.config = config

        self.trigger_tracker = TriggerCrawlTracker()

        self.backlog_tracker = BacklogTracker()

        self.activity_stats = ActivityStats(gl_instance=self.config.gl_instance.name,
                                             trigger_frequency=int(self.config.trigger_frequency / 60))

        self.projects_events = ProjectsEventsTracker()

        self.processing_timer = CrawlProcessingTimer()

        self.current_hour: int = self.config.crawler_start_hour.hour

        self.response_content: Optional[Union[list[GitLabProject], list[GitLabEvent]]] = None
        self.response_headers: Optional[dict] = None
        self.response_status: Optional[int] = None
        self.response_text: Optional[str] = None
        self.response_time: Optional[float] = None

        self.project_deleted: bool = False

        self.last_project_fetch_time: Optional[datetime] = None

        self.running: bool = True
        self.event_loop = asyncio.new_event_loop()


    def start(self):
        self.event_loop.add_signal_handler(signal.SIGINT, self.handle_stop_signal, None, None)
        self.event_loop.add_signal_handler(signal.SIGTERM, self.handle_stop_signal, None, None)
        asyncio.set_event_loop(self.event_loop)

        logger.info("Starting GitLab crawler")
        self.event_loop.create_task(self.run())
        self.event_loop.run_forever()


    async def run(self):
        """Run the crawler asynchronously until the internal `running` flag is set to False."""

        logger.info(f"Instance: {self.config.gl_instance.name} | "
                    f"trigger frequency: {int(self.config.trigger_frequency / 60)} minutes | "
                    f"timeout value: {self.config.timeout_value} minutes")

        # Create the folders for events and activity stats
        self.initialize_folders_and_filepath(separate_folders=True)

        logger.info('Starting recovery of backlog projects')
        await self.recover_backlog_projects()
        backlog_event_recovery_start = datetime.now()

        while self.running:

            current_time = datetime.now(timezone.utc)
            if (current_time - self.last_project_fetch_time).total_seconds() >= self.config.trigger_frequency:
                # Code block executed every 'trigger_frequency' seconds

                projects_list_exhausted = self.projects_events.is_project_list_exhausted()
                first_processing_measured = self.processing_timer.is_first_processing_measured()

                if projects_list_exhausted and first_processing_measured:
                    # Code block executed once the crawler backlog recovery is finished and a subsequent crawl was complete

                    nb_recovered_projects = self.trigger_tracker.get_nb_recovered_projects()
                    projects_recovery_time = self.trigger_tracker.get_projects_recovery_time()
                    projects_response_time = self.trigger_tracker.get_projects_response_time()
                    self.activity_stats.set_trigger_projects(nb_recovered_projects, projects_recovery_time,
                                                              projects_response_time)

                    nb_recovered_events = self.trigger_tracker.get_nb_recovered_events()
                    events_response_time = self.trigger_tracker.get_events_response_time()

                    self.activity_stats.set_trigger_events(nb_events=nb_recovered_events,
                                                            list_response_time=events_response_time)

                    processing_time = self.processing_timer.get_elapsed_time()
                    nb_request_errors = self.trigger_tracker.get_nb_request_errors()

                    self.activity_stats.set_trigger_stats(current_time=current_time,
                                                           processing_time=processing_time,
                                                           nb_errors=nb_request_errors)

                    self.activity_stats.disk_write_json()

                    if self.processing_timer.is_first_processing_measured():
                        # Write activity stats in a CSV file
                        _, projects_avg_time = total_and_avg_time(self.trigger_tracker.get_projects_response_time())
                        _, events_avg_time = total_and_avg_time(self.trigger_tracker.get_events_response_time())

                        self.activity_stats.append_csv_stats(current_time=current_time,
                                                              nb_projects=nb_recovered_projects,
                                                              projects_avg_time=projects_avg_time,
                                                              nb_events=nb_recovered_events,
                                                              events_avg_time=events_avg_time,
                                                              processing_time=processing_time,
                                                              nb_errors=nb_request_errors)

                        self.activity_stats.disk_write_csv()

                    self.trigger_tracker.reset_tracker()

                    logger.info(f'Number recovered projects: {nb_recovered_projects} | '
                                f'number recovered events: {nb_recovered_events} | '
                                f'total recovery time: {processing_time}')

                await self.fetch_and_store_projects()
                nb_projects_left = self.projects_events.get_nb_projects()
                self.trigger_tracker.increase_nb_recovered_projects(nb_projects_left)

            nb_backlog_projects = self.backlog_tracker.get_nb_backlog_projects()
            if nb_backlog_projects == 0 and not self.backlog_tracker.is_recovery_finished():
                # Backlog event recovery complete
                self.backlog_tracker.set_recovery_finished()

                backlog_event_recovery_end = datetime.now()
                recovery_duration = str(backlog_event_recovery_end - backlog_event_recovery_start)

                nb_backlog_events = self.backlog_tracker.get_nb_events()
                events_response_time = self.trigger_tracker.get_events_response_time()
                self.activity_stats.set_backlog_events(time_elapsed=recovery_duration, nb_events=nb_backlog_events,
                                                        response_time=events_response_time)

                self.activity_stats.disk_write_json()

                self.trigger_tracker.reset_tracker()

                logger.info(f'Backlog recovery finished')
                logger.info(f'Backlog events recovery time: {recovery_duration}')
                logger.info(f'Number backlog events: {nb_backlog_events}')

            current_hour = current_time.hour
            if self.current_hour != current_hour:
                # At the end of the hour, write event file to disk if new events were recovered
                self.current_hour = current_hour
                self.disk_write_events()

            if not self.projects_events.is_project_list_exhausted():
                # Fetch events as long as there are projects left
                await asyncio.sleep(0.001)
                await self.fetch_events()

            else:
                if self.processing_timer.is_timer_started():
                    self.processing_timer.stop_timer()

                # Sleep until next crawl time
                next_fetch_time = self.last_project_fetch_time + timedelta(seconds=self.config.trigger_frequency)
                current_time = datetime.now(timezone.utc)
                time_until_next_fetch = next_fetch_time - current_time

                logger.info(f'No more events left to fetch. Sleeping until: '
                            f'{datetime_to_str(next_fetch_time, utc=True)}')
                await asyncio.sleep(time_until_next_fetch.total_seconds())

                self.processing_timer.start_timer()


    async def stop(self):
        """Stop the crawler. This method waits for current I/O operations to finish and ensures the
        validity of the current JSON file."""

        self.running = False

        if self.projects_events.are_new_events_found():
            self.disk_write_events()
            logger.info('All new events were successfully written to disk')
        else:
            logger.info('No new events were retrieved')

        self.event_loop.stop()

        logger.info("Shutdown")

    # noinspection PyUnusedLocal
    def handle_stop_signal(self, sig, frame):
        """Handler for SIGINT and SIGTERM signals. This method creates a new asynchronous task to initiate the
            shutdown procedure for this crawler."""

        logger.info("Terminating crawler")
        self.event_loop.create_task(self.stop())


    async def shutdown(self):
        await asyncio.sleep(0.001)
        self.handle_stop_signal(None, None)


    async def recover_backlog_projects(self):
        """
        Recover all the projects that were updated since the beginning of the previous hour
        Executed at crawler launch
        """
        time_window_before = remove_milliseconds(datetime.now(timezone.utc))
        time_window_after = self.config.crawler_start_hour - timedelta(hours=1)

        time_window_length = time_window_before - time_window_after
        # Remove milliseconds
        time_window_formatted = str(time_window_length).rsplit('.', 1)[0]

        if self.config.verbose:
            logger.info(f'backlog time window: {time_window_formatted}')

        await self.fetch_and_store_projects(after=time_window_after)

        nb_backlog_projects = self.projects_events.get_nb_projects()
        response_time = self.trigger_tracker.get_projects_response_time()
        recovery_time = self.trigger_tracker.get_projects_recovery_time()
        self.activity_stats.set_backlog_projects(time_window=time_window_formatted, nb_projects=nb_backlog_projects,
                                                  recovery_time=recovery_time, list_response_time=response_time)
        self.activity_stats.disk_write_json()

        self.backlog_tracker.set_nb_projects(nb_backlog_projects)

        _, avg_response_time = total_and_avg_time(response_time)


    async def fetch_events(self):
        """Fetch the latest events from the by timestamp (date and hour). A `stop event`
        is set at the end to notify the end of I/O operations. This method is not thread-safe."""

        project_id = self.projects_events.pop_project_id()

        events_url = self.config.gl_instance.url.rsplit('?')[0]
        events_url += f'/{project_id}/events?per_page={self.config.page_limit}'

        async with aiohttp.ClientSession() as session:
            await self.api_call(events_url, project_id=project_id, session=session)

            recovered_events = self.response_content
            recent_events, all_events_retrieved = self.check_all_events_retrieved(recovered_events)
            list_response_time = [self.response_time]

            if not all_events_retrieved:
                # Send subsequent requests in order to recover all new events
                recent_events = []
                request_page_nb = 1
                paged_url = events_url.rsplit('?')[0]
                paged_url += f'?per_page=100'

                while not all_events_retrieved:
                    current_paged_url = f'{paged_url}&page={request_page_nb}'

                    await self.api_call(current_paged_url, project_id=project_id, session=session)

                    recovered_events = self.response_content

                    events, all_events_retrieved = self.check_all_events_retrieved(recovered_events, paged_request=True)
                    recent_events.extend(events)

                    list_response_time.append(self.response_time)

                    request_page_nb += 1

        if not self.backlog_tracker.is_recovery_finished():
            self.backlog_tracker.incr_nb_events(len(recent_events))

        if self.config.verbose:
            logger.info(f'Found {len(recent_events)} event(s) for project {project_id} | '
                        f'total response time: {round(sum(list_response_time), 2)} sec | '
                        f'average response time: {round(sum(list_response_time) / len(list_response_time), 2)} sec')

        self.trigger_tracker.increase_nb_recovered_events(len(recent_events))
        self.trigger_tracker.extend_events_response_time(list_response_time)

        for event in recent_events:
            self.projects_events.store_event(event, project_id)

        if not self.backlog_tracker.are_all_projects_processed():
            self.backlog_tracker.decr_nb_projects()


    def check_all_events_retrieved(self, events: list[GitLabEvent], paged_request = False):
        """
        Check if all the latest events were retrieved for a given project.
        All events recovered when:
            - a known event is found
            - event is older than the crawler launch hour
        """
        retrieved_all_recent_events = False
        recent_events: list[GitLabEvent] = []

        if self.project_deleted:
            retrieved_all_recent_events = True

        else:
            for event in events:
                event_id = object_id(event)
                event_date = event_creation_date(event)

                if not self.projects_events.is_event_known(event_id) and event_date >= self.config.crawler_start_hour:
                    recent_events.append(event)
                else:
                    retrieved_all_recent_events = True
                    break

        if not retrieved_all_recent_events:
            # Due to the GitLab's API behavior, the requested number of events per page is not always respected
            # Example: if 100 events are requested per page, it is possible to obtain between 100 - 90 events
            # The number of obtained events seems arbitrary (based on the observations made thus far)
            # The "number_of_events / 2" condition ensures that the requested list is exhausted
            if paged_request:
                retrieved_all_recent_events: bool = len(events) < 100 / 2
            else:
                retrieved_all_recent_events: bool = len(events) < self.config.page_limit / 2

        return recent_events, retrieved_all_recent_events


    async def fetch_and_store_projects(self, after: datetime = None):
        """
        Recover projects updated since the last fetch, or the beginning of the previous hour of crawler launch
        The recovery is done with parallelized tasks
        """
        current_time = datetime.now(timezone.utc)

        if after is None:
            after = self.last_project_fetch_time

        timer_start = time.time()

        projects: list[GitLabProject] = []
        list_response_time: list[float] = []

        async with (aiohttp.ClientSession() as session):
            # Recover first page of projects
            first_page, total_nb_pages, nb_projects, first_response_time = await self.fetch_projects(after=after,
                                                                before=current_time, session=session, page_nb=1)

            if self.config.verbose:
                logger.info(f'Expected number of projects: {nb_projects}')

            projects.extend(first_page)
            list_response_time.append(first_response_time)

            if total_nb_pages > 1:
                # Create parallel tasks for faster recovery of projects in order to minimize project loss probability
                tasks = [
                    self.fetch_projects(after=after, before=current_time, session=session, page_nb=page)
                    for page in range(2, total_nb_pages + 1)
                ]
                results = await asyncio.gather(*tasks)

                for paged_projects, _, _, response_time in results:
                    projects.extend(paged_projects)
                    list_response_time.append(response_time)

        timer_end = time.time()
        total_time = timer_end - timer_start

        self.last_project_fetch_time = current_time

        _, avg_response_time = total_and_avg_time(list_response_time)
        logger.info(f'Fetched {len(projects)} projects | '
                    f'total processing time: {round(total_time, 2)} sec | '
                    f'average response time: {round(avg_response_time, 2)} sec')

        self.trigger_tracker.set_projects_response_time(list_response_time)
        self.trigger_tracker.set_projects_recovery_time(total_time)
        self.projects_events.add_projects(projects)

        return total_time


    async def fetch_projects(self, *, after: datetime, before: datetime, session, page_nb: int):
        """
        Create the endpoint url for recovering recently updated projects
        """
        before_formatted = datetime_to_str(before, utc=True)
        after_formatted = datetime_to_str(after, utc=True)
        projects_url = (f'{self.config.gl_instance.url}&sort=asc&simple=true&last_activity_after={after_formatted}&'
                        f'last_activity_before={before_formatted}&per_page=100&page={page_nb}')

        await self.api_call(projects_url, session)

        projects = cast(list[GitLabEvent], self.response_content)

        # Response header fields that indicate the total number of projects and pages based on the url filters
        nb_pages = int(self.response_headers.get('x-total-pages'))
        nb_expected_projects = int(self.response_headers.get('x-total'))

        return projects, nb_pages, nb_expected_projects, self.response_time


    async def api_call(self, url: str, session, project_id: int = None):
        """
        Send request to the API in order to obtain projects/events
        Repeat the request if failed, until the response is received or the timeout is reached
        """
        request_successful = False

        timeout_delay = timedelta(minutes=self.config.timeout_value)

        start_time = datetime.now()
        current_time = start_time

        while not request_successful and current_time - start_time < timeout_delay:
            try:
                current_time = datetime.now()
                if current_time - start_time >= timeout_delay:
                    logger.error(f"Request fetching exceeded the timeout value: {self.config.timeout_value} minute(s)")
                    await self.shutdown()
                    await asyncio.sleep(1)

                timer_start = time.time()

                async with session.get(url, headers=self.config.request_header) as response:
                    self.response_text = await response.text()
                    self.response_headers = response.headers
                    self.response_status = response.status

                    self.response_time = time.time() - timer_start

                    response.raise_for_status()

                    self.response_content = await response.json()

                    self.project_deleted = False
                    request_successful = True

            except aiohttp.ClientResponseError as e:
                if e.status == 404:
                    self.project_deleted = True
                    request_successful = True
                    if self.config.verbose:
                        logger.info(f'Project {project_id} was deleted')

                elif e.status == 401:
                    logger.error(f'Request error: {e.status} Unauthorized')
                    await self.shutdown()
                    await asyncio.sleep(1)

                elif e.status == 403:
                    logger.error(f'Request error: {e.status} Forbidden')
                    logger.error(f'Response content:\n{self.response_text}')
                    logger.error(f'Endpoint: {url}')
                    await self.shutdown()
                    await asyncio.sleep(1)

                elif e.status == 400:
                    # Http error encountered after running the crawler for more than 24H
                    # Error encountered despite the static format of the request
                    # Potentially caused by an internal error of GL instance servers
                    logger.error(f'Request error: {e.status} Bad Request')
                    logger.error(f'Endpoint: {url}')
                    logger.error(f'Response content:\n{self.response_text}')
                    logger.error(f'Response headers:\n{self.response_headers}')
                    logger.error(f'Retrying request after {self.config.request_delay} seconds')
                    self.trigger_tracker.increment_nb_request_errors()
                    await asyncio.sleep(self.config.request_delay)

                elif e.status == 500:
                    logger.error(f'Request error: {e.status} Internal Error')
                    self.trigger_tracker.increment_nb_request_errors()
                    await asyncio.sleep(self.config.request_delay)

            except (asyncio.TimeoutError, aiohttp.ClientConnectionError, aiohttp.ClientPayloadError) as exception:
                logger.error(f'Request error: {type(exception).__name__}')
                self.trigger_tracker.increment_nb_request_errors()
                await asyncio.sleep(self.config.request_delay)


    def disk_write_events(self):
        """
        Write events to disk if new events are found for a timestamp (date and hour)
        """
        if self.projects_events.are_new_events_found():
            current_time = datetime.now(timezone.utc)
            known_timestamps = self.projects_events.get_known_timestamps()
            disk_write_start = time.time()

            for timestamp in known_timestamps:
                if self.projects_events.is_timestamp_updated(timestamp):

                    events_file_path = os.path.join(self.config.events_dir, f'{timestamp}h.json')

                    target_timestamp_events = self.projects_events.get_hourly_events(timestamp)
                    nb_written_events = len(target_timestamp_events)
                    logger.info(f'Writing {nb_written_events} event(s) to {events_file_path}')

                    start = time.time()
                    with open(events_file_path, 'wb') as events_file:
                        events_file.write(orjson.dumps(target_timestamp_events))
                    end = time.time()

                    disk_write_duration = end - start
                    self.activity_stats.add_disk_write_entry(timestamp, nb_written_events, disk_write_duration)
                    self.projects_events.set_no_new_events(timestamp)

            disk_write_end = time.time()

            total_write_duration = disk_write_end - disk_write_start
            current_hour = reset_hour_beginning(current_time)
            current_hour_str = datetime_to_str(current_hour)

            self.activity_stats.set_overall_disk_write(current_hour_str, total_write_duration)
            self.activity_stats.disk_write_json()


    def initialize_folders_and_filepath(self, separate_folders: bool = False):
        """
        Create the folders for events and forge activity stats
        """
        if separate_folders:
            self.config.stats_dir = os.path.join(self.config.data_dir, self.config.gl_instance.raw_name,
                                                 'activity_stats')
            self.config.events_dir = os.path.join(self.config.data_dir, self.config.gl_instance.raw_name, 'events')

        os.makedirs(self.config.stats_dir, exist_ok=True)
        os.makedirs(self.config.events_dir, exist_ok=True)

        datetime_formatted = datetime_to_hour(self.config.crawler_start_hour)
        json_path = os.path.join(self.config.stats_dir, f'{datetime_formatted}h.json')
        csv_path = os.path.join(self.config.stats_dir, f'{datetime_formatted}h.csv')

        self.activity_stats.set_file_path(json_path=json_path, csv_path=csv_path)