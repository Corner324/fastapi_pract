import pytest
from unittest.mock import AsyncMock, patch
from src.cache import get_redis_client, cache_response, clear_cache
import redis.asyncio as redis


@pytest.fixture
def mock_redis_client():
    mock_client = AsyncMock(spec=redis.Redis)
    mock_client.ping = AsyncMock(return_value=True)
    mock_client.get = AsyncMock(return_value=None)
    mock_client.setex = AsyncMock(return_value=True)
    mock_client.keys = AsyncMock(return_value=[])
    mock_client.delete = AsyncMock(return_value=0)
    return mock_client


@pytest.mark.asyncio
async def test_get_redis_client_success(mock_redis_client):
    with patch("src.cache.redis.Redis", return_value=mock_redis_client):
        client = await get_redis_client()
        assert client is not None
        mock_redis_client.ping.assert_awaited_once()
        assert client is mock_redis_client


@pytest.mark.asyncio
async def test_cache_response_decorator_hit(mock_redis_client):
    cached_data = {"data": "cached"}
    mock_redis_client.get = AsyncMock(return_value='{"data": "cached"}')

    @cache_response(key_prefix="test_prefix")
    async def mock_func(arg1, arg2):
        return {"data": "original"}

    with patch("src.cache.get_redis_client", return_value=mock_redis_client):
        result = await mock_func("value1", arg2="value2")
        assert result == cached_data
        mock_redis_client.get.assert_awaited_once()
        mock_redis_client.setex.assert_not_called()


@pytest.mark.asyncio
async def test_clear_cache_success(mock_redis_client):
    mock_redis_client.keys = AsyncMock(return_value=["key1", "key2"])
    mock_redis_client.delete = AsyncMock(return_value=2)

    with patch("src.cache.get_redis_client", return_value=mock_redis_client):
        success = await clear_cache("test_pattern:*")
        assert success is True
        mock_redis_client.keys.assert_awaited_once_with("test_pattern:*")
        mock_redis_client.delete.assert_awaited_once_with("key1", "key2")
