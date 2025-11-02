from datetime import datetime
from pathlib import Path

from gitlab_crawler.types_formats import JSON_DATE_FORMAT


def find_project_root(marker: str ='requirements.txt') -> Path:
    """
    Find the path of the project's root directory
    """
    path = Path().resolve()
    for parent in [path] + list(path.parents):
        if (parent / marker).exists():
            return parent
    raise FileNotFoundError(f"Could not find {marker} in any parent directories")


def total_and_avg_time(response_time: list[float]):
    """
    Return total and average time from a list of request response duration (measured in seconds)
    """
    total_time = sum(response_time)

    if len(response_time) > 0:
        avg_time = total_time / len(response_time)
    else:
        avg_time = 0

    return total_time, avg_time


def remove_milliseconds(dt: datetime):
    return dt.replace(microsecond=0)


def reset_hour_beginning(dt: datetime):
    return dt.replace(minute=0, second=0, microsecond=0)


def datetime_to_str(dt: datetime, utc: bool = False):
    """
    Format a datetime to a string used as a timestamp in JSON/CSV files
    Removes the UTC offset if present
    """
    res_dt = remove_milliseconds(dt)
    res_dt = res_dt.isoformat()

    if not utc:
        res_dt = res_dt.rsplit('+', 1)[0]

    return res_dt


def datetime_to_hour(dt: datetime):
    return dt.strftime(JSON_DATE_FORMAT)