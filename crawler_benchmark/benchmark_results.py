import os
import argparse
import re
from pathlib import Path
from typing import Optional

master_file_name: str = '0_master.txt'
log_folder_path: Optional[str] = None


def find_project_root(marker: str ='requirements.txt') -> Path:
    """
    Find the path of the project's root directory
    """
    path = Path().resolve()
    for parent in [path] + list(path.parents):
        if (parent / marker).exists():
            return parent
    raise FileNotFoundError(f"Could not find {marker} in any parent directories")


project_root_parent = find_project_root().parent
benchmark_results_path = os.path.join(project_root_parent, 'crawler-benchmark')


def get_failed_instances(target_timestamp: str):
    global master_file_name
    global log_folder_path
    global benchmark_results_path

    if target_timestamp is None:
        last_log_folder = sorted(os.listdir(benchmark_results_path))[-1]
    else:
        last_log_folder = target_timestamp

    expected_name_format = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"
    if not re.match(expected_name_format, last_log_folder):
        raise ValueError('Path does not contain the expected folder name')

    log_folder_path = os.path.join(benchmark_results_path, last_log_folder)

    master_file_path = os.path.join(log_folder_path, master_file_name)

    with open(master_file_path, 'r') as f:
        failed_instances = [line.split()[0] for line in f.readlines()[1:]]

    return failed_instances


def check_crawler_execution(target_timestamp: str):
    global master_file_name
    global log_folder_path
    global benchmark_results_path

    failed_instances = get_failed_instances(target_timestamp)

    accepted_logs = ['No more events left to fetch', 'number recovered projects', 'Starting', 'Fetched', 'Backlog',
                     'Shutdown', 'Request timed out', 'Connection failed']

    failed_executions: list[str] = []
    for instance_log_file in os.listdir(log_folder_path):
        instance_name = instance_log_file.replace('.log', '')
        if instance_log_file != master_file_name and instance_name not in failed_instances:
            log_file_path = os.path.join(log_folder_path, instance_log_file)
            with open(log_file_path, 'r') as f:
                instance_logs = f.readlines()

            last_log = instance_logs[-1]
            if all(message not in last_log for message in accepted_logs):
                failed_executions.append('\n'.join(instance_logs))

    if len(failed_executions) == 0:
        print('No unexpected errors found')
    else:
        for log in failed_executions:
            print(log)
            print('_' * 80)


def print_failed_instances(target_timestamp: str = None):
    global master_file_name
    global log_folder_path

    failed_instances = get_failed_instances(target_timestamp)

    for instance in failed_instances:
        failed_instance_path = os.path.join(log_folder_path, f'{instance}.log')

        with open(failed_instance_path) as f:
            instance_logs = f.readlines()

        for log_message in instance_logs:
            print(log_message)
        print('_' * 80)


################################################################################
def get_args():
    """
    Command line parsing and help.
    """
    my_parser = argparse.ArgumentParser(description="Check the last execution of the GitLab crawler on all the targeted"
                                                    " instances. List all the crawler instances that encounter an "
                                                    "unhandled exceptions or fail due to request error (e.g., timeout, "
                                                    "forbidden request). "
                                                    "To be launched after the execution of the benchmark")

    default_timestamp = None
    my_parser.add_argument('-t', '--timestamp', type=str, default=default_timestamp,
                           help='Target timestamp to be analyzed. Defaults to the last executed benchmark.')


    return my_parser.parse_args()

################################################################################
if __name__ == "__main__":
    my_args = get_args()

    timestamp = my_args.timestamp

    print('*' * 40 + 'Crawler fail' + '*' * 40)
    check_crawler_execution(timestamp)
    print('*' * 40 + 'Failed instances' + '*' * 40)
    print_failed_instances(timestamp)