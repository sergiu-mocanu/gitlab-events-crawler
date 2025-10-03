from dateutil import parser
from typing import Dict, Any

JSON_DATE_FORMAT: str = "%Y-%m-%d--%-k"
object_id = lambda e: e["id"]
GL_DATE_FORMAT: str = "%Y-%m-%dT%H:%M:%S.%fZ"
event_creation_date = lambda e: parser.parse(e["created_at"])

GitLabEvent = Dict[str, Any]
GitLabProject = Dict[str, Any]
Event_ID = int
Project_ID = int
Timestamp = str