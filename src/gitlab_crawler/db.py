from __future__ import annotations

from typing import Iterable
from datetime import datetime

import asyncpg
import json

from gitlab_crawler.types_formats import GitLabEvent


class Database:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn)

    async def close(self):
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def init_schema(self):
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS gitlab_events (
                    id  BIGINT PRIMARY KEY,
                    project_id  BIGINT NOT NULL,
                    created_at  TIMESTAMPTZ NOT NULL,
                    event_type  TEXT NOT NULL,
                    payload  JSONB NOT NULL,
                    inserted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_gitlab_events_project_created
                    ON gitlab_events (project_id, created_at DESC);
                '''
            )


    async def insert_events(self, events: Iterable[GitLabEvent]):
        """
        Insert a batch of GitLab events into the database.
        Assumes each event dict has keys: 'id', 'project_id', 'created_at' and 'action_name'.
        """
        events = list(events)
        if not events:
            return

        assert self._pool is not None
        rows = []

        for ev in events:
            event_id = int(ev['id'])
            project_id = int(ev['project_id'])
            created_at_str = ev['created_at']
            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            event_type = ev['action_name']

            rows.append((event_id, project_id, created_at, event_type, json.dumps(ev)))

        async with self._pool.acquire() as conn:
            await conn.executemany(
                '''
                INSERT INTO gitlab_events (id, project_id, created_at, event_type, payload)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO NOTHING;
                ''',
                rows,
            )

