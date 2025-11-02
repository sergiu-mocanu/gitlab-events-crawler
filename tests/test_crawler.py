from unittest.mock import AsyncMock, MagicMock

import pytest
import asyncio
import aiohttp
from aioresponses import aioresponses

from gitlab_crawler.crawler import GitLabInstance, GitLabCrawler

pytestmark = pytest.mark.asyncio


@pytest.fixture
def fake_url() -> str:
    return 'https://example.com'


@pytest.fixture(autouse=True)
def init_crawler() -> GitLabCrawler:
    instance = GitLabInstance('gitlab.com')
    config = GitLabCrawler.CrawlerConfig(
        gl_instance=instance, trigger_frequency=1,
        timeout_value=1, delay=0, verbose=False,
        data_dir='tmp'
    )
    return GitLabCrawler(config)


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    async def no_sleep(_):
        return None
    monkeypatch.setattr(asyncio, 'sleep', no_sleep)


async def test_send_request_returns_json(init_crawler, fake_url):
    """Verify that crawler processes fetched GitLab projects correctly."""
    crawler = init_crawler
    fake_payload = {'id': 1, 'name': 'Demo'}

    with aioresponses() as mock:
        mock.get(fake_url, payload=fake_payload, status=200)

        async with aiohttp.ClientSession() as session:
            await crawler.api_call(fake_url, session)

    assert crawler.response_content == fake_payload
    assert crawler.response_status == 200
    assert not crawler.project_deleted


async def test_api_call_handles_404(init_crawler, fake_url):
    """Verify that crawler handles 404 errors."""
    crawler = init_crawler
    crawler.shutdown = AsyncMock()

    with aioresponses() as mock:
        mock.get(fake_url, status=404, body='Not Found')

        async with aiohttp.ClientSession() as session:
            await crawler.api_call(fake_url, session)

    assert crawler.project_deleted is True
    crawler.shutdown.assert_not_called()
    assert crawler.trigger_tracker.get_nb_request_errors() == 0


@pytest.mark.parametrize(
    'status_code,message',
    [
        (401, 'Unauthorized'),
        (403, 'Forbidden'),
    ],
)
async def test_api_call_handles_401_403(init_crawler, fake_url, status_code: int, message: str):
    """Verify that crawler handles 401 and 403 errors."""
    crawler = init_crawler
    crawler.shutdown = AsyncMock()

    with aioresponses() as mock:
        mock.get(fake_url, status=status_code, body=message)

        async with aiohttp.ClientSession() as session:
            await crawler.api_call(fake_url, session)

    crawler.shutdown.assert_called_once()


@pytest.mark.parametrize(
    'status_code,message',
    [
        (400, 'Bad Request'),
        (500, 'Internal Error'),
    ],
)
async def test_api_call_handles_400_500(init_crawler, fake_url, status_code: int, message: str):
    """Verify that crawler handles and records 400 and 500 errors and retries the request."""
    crawler = init_crawler
    crawler.shutdown = AsyncMock()
    crawler.config.request_delay = 0

    with aioresponses() as mock:
        mock.get(fake_url, status=status_code, body=message)

        fake_payload = {'result': 'ok'}
        mock.get(fake_url, status=200, payload=fake_payload)

        async with aiohttp.ClientSession() as session:
            await crawler.api_call(fake_url, session)

    assert crawler.trigger_tracker.get_nb_request_errors() == 1
    crawler.shutdown.assert_not_called()
    assert crawler.response_content == fake_payload
    assert crawler.project_deleted is False


@pytest.mark.parametrize('exc_type', [
    asyncio.TimeoutError,
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
])
async def test_api_call_handles_network_errors(init_crawler, fake_url, exc_type):
    """Verify that crawler retries and records errors on network-level exceptions."""
    crawler = init_crawler
    crawler.shutdown = AsyncMock()
    crawler.config.timeout_value = 0.0001
    crawler.config.request_delay = 0

    mock_session = MagicMock()
    mock_session.get.side_effect = exc_type()

    await crawler.api_call(fake_url, mock_session)

    assert crawler.trigger_tracker.get_nb_request_errors() >= 1
    crawler.shutdown.assert_called_once()
    assert crawler.project_deleted is False



