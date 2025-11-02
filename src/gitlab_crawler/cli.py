import argparse
import os

from gitlab_crawler.utils import find_project_root
from gitlab_crawler.models import GitLabToken, GitLabInstance

from gitlab_crawler.crawler import GitLabCrawler


def get_args():
    """
    Command line parsing and help.
    """

    my_parser = argparse.ArgumentParser(description="Recover GitLab's most recent events every n minutes, as well as "
                                                    "time measurements of the API answers and counters that show the "
                                                    "activity of the crawled forge instance.")

    default_instance = 'gitlab.com'
    my_parser.add_argument("-i", "--instance", type=str, default=default_instance,
                           help=f"Choose the target GitLab instance. Defaults to {default_instance}.")

    default_trigger_frequency = 60
    my_parser.add_argument("-fr", "--frequency", type=int, default=default_trigger_frequency,
                           help=f"Choose the trigger frequency (in minutes) between forge crawls. "
                                f"Defaults to {default_trigger_frequency} minutes.")

    default_timeout_value = 60
    my_parser.add_argument("-tm", "--timeout", type=int, default=default_timeout_value,
                           help=f"Choose the maximum timeout value (in minutes) for successfully sending a request. "
                                f"Defaults to {default_timeout_value} minutes.")

    default_request_delay = 0
    my_parser.add_argument("-d", "--delay", type=int, default=default_request_delay,
                           help=f"Choose the delay after a failed request (in seconds). "
                                f"Defaults to {default_timeout_value} seconds.")

    my_parser.add_argument("-v", "--verbose", action="store_true")


    project_root_parent = find_project_root().parent
    gl_data_dir = os.path.join(project_root_parent, 'gitlab_events')
    my_parser.add_argument("-p", "--target_dir", type=str, default=gl_data_dir,
                           help=f"Choose the target folder for storing the GitLab events. Defaults to the parent folder"
                                f"of the project: {gl_data_dir}.")

    my_parser.add_argument("-to", "--token", type=str,
                           help=f"Insert the path to the GitLab token used for authentication. Defaults to no token.")

    return my_parser.parse_args()

################################################################################
if __name__ == "__main__":
    my_args = get_args()

    instance_name = my_args.instance
    frequency = my_args.frequency
    timeout = my_args.timeout
    request_delay = my_args.delay
    verbose_mode = my_args.verbose
    target_dir = my_args.target_dir

    if my_args.token is not None:
        token_path = os.path.expanduser(my_args.token)
        gitlab_token = GitLabToken(token_path)
    else:
        gitlab_token = None

    gitlab_instance = GitLabInstance(instance_name)

    crawler_config = GitLabCrawler.CrawlerConfig(gl_instance=gitlab_instance, trigger_frequency=frequency,
                                                 timeout_value=timeout, delay=request_delay, verbose=verbose_mode,
                                                 gl_token=gitlab_token, data_dir=target_dir)
    crawler = GitLabCrawler(crawler_config)
    crawler.start()