import json

import pytest
import pytest_asyncio
import asyncpg

from gitlab_crawler.db_dsn import db_dsn
from gitlab_crawler.db import Database

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def db_conn():
    conn = await asyncpg.connect(db_dsn)
    try:
        # Clean table before each test
        await conn.execute('TRUNCATE TABLE gitlab_events')
        yield conn
    finally:
        await conn.close()


@pytestmark
async def test_init_schema_creates_table(db_conn):
    """Ensure that the database schema initialization creates the expected tables."""
    db = Database(db_dsn)
    await db.connect()
    await db.init_schema()

    rows = await db_conn.fetch(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'gitlab_events';"
    )
    assert len(rows) == 1


@pytestmark
async def test_insert_events_inserts_rows(db_conn):
    """Verify that inserting events through the Database layer persists rows correctly."""
    db = Database(db_dsn)
    await db.connect()

    fake_event = {
        "id": 1,
        "created_at": "2025-11-13T13:00:00+00:00",
        "project_id": 123,
        "action_name": "pushed"
    }

    list_events = [fake_event]

    await db.insert_events(list_events)
    rows = await db_conn.fetch(
        '''
        SELECT id, project_id, created_at, event_type, payload
        FROM gitlab_events
        '''
    )
    assert len(rows) == 1

    row = rows[0]
    assert row['id'] == fake_event['id']
    assert row['project_id'] == fake_event['project_id']
    assert row['created_at'].isoformat() == fake_event['created_at']
    assert row['event_type'] == fake_event['action_name']

    payload = json.loads(row['payload'])
    assert payload == fake_event


@pytestmark
async def test_insert_events_no_duplicates(db_conn):
    """Verify that inserting the same event won't create duplicate data."""
    db = Database(db_dsn)
    await db.connect()

    fake_event = {
        "id": 1,
        "created_at": "2025-11-13T13:00:00+00:00",
        "project_id": 123,
        "action_name": "pushed"
    }

    list_duplicate_events = [fake_event, fake_event]
    await db.insert_events(list_duplicate_events)
    rows = await db_conn.fetch(
        '''
        SELECT *
        FROM gitlab_events
        '''
    )
    assert len(rows) == 1
