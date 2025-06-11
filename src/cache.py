import json
import asyncio
from datetime import datetime, time, timedelta
from typing import Optional, Callable, Any
import redis.asyncio as redis
from redis.asyncio import Redis
from config import REDIS_HOST, REDIS_PORT, REDIS_DB
import logging
import functools

logger = logging.getLogger(__name__)

redis_client: Optional[Redis] = None


async def get_redis_client() -> Redis:
    global redis_client
    if redis_client is None or not redis_client.ping():
        try:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                port=int(REDIS_PORT),
                db=int(REDIS_DB),
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            await redis_client.ping()
            logger.info("Успешное подключение к Redis")
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis: {e}")
            return None
    return redis_client


def get_ttl_until_daily_reset() -> int:
    now = datetime.now()
    # Время сброса: 14:11
    reset_time = time(14, 11, 0)

    if now.time() >= reset_time:
        next_reset = datetime.combine(now.date() + timedelta(days=1), reset_time)
    else:
        next_reset = datetime.combine(now.date(), reset_time)

    ttl = (next_reset - now).total_seconds()
    return max(1, int(ttl))


def cache_response(
    key_prefix: str, expiration_seconds: Optional[int] = None
) -> Callable[[Any], Any]:
    def _cache_response(func: Callable[..., Any]) -> Any:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            try:
                redis_conn = await get_redis_client()
                
                if redis_conn is None:
                    logger.warning(f"Redis недоступен, выполнение {func.__name__} без кэширования")
                    return await func(*args, **kwargs)

                cache_args = {k: v for k, v in kwargs.items() if k != "db"}
                cache_key = f"{key_prefix}:{func.__name__}:{json.dumps(cache_args, sort_keys=True, default=str)}"

                try:
                    cached_data = await redis_conn.get(cache_key)
                    if cached_data:
                        logger.info(f"Данные получены из кеша для ключа: {cache_key}")
                        return json.loads(cached_data)
                except Exception as e:
                    logger.error(f"Ошибка при получении данных из кеша: {e}")

                response_data = await func(*args, **kwargs)

                ttl = (
                    expiration_seconds
                    if expiration_seconds is not None
                    else get_ttl_until_daily_reset()
                )

                try:
                    await redis_conn.setex(
                        cache_key, ttl, json.dumps(response_data, default=str)
                    )
                    logger.info(
                        f"Данные закешированы для ключа: {cache_key} с TTL: {ttl} секунд"
                    )
                except Exception as e:
                    logger.error(f"Ошибка при сохранении данных в кеш: {e}")
                
                return response_data
            except Exception as e:
                logger.error(f"Непредвиденная ошибка в декораторе cache_response: {e}")
                return await func(*args, **kwargs)

        return wrapper

    return _cache_response


async def clear_cache(pattern: str = "*") -> bool:
    """Очищает кеш по заданному шаблону."""
    try:
        redis_conn = await get_redis_client()
        if redis_conn is None:
            logger.error("Невозможно очистить кеш: Redis недоступен")
            return False
            
        keys = await redis_conn.keys(pattern)
        if keys:
            await redis_conn.delete(*keys)
            logger.info(f"Очищено {len(keys)} ключей кеша по шаблону '{pattern}'")
        else:
            logger.info(f"Ключи по шаблону '{pattern}' не найдены")
        return True
    except Exception as e:
        logger.error(f"Ошибка при очистке кеша: {e}")
        return False


async def schedule_cache_reset():
    """Планирует сброс кэша каждый день в 14:11."""
    while True:
        ttl = get_ttl_until_daily_reset()
        logger.info(f"Следующий сброс кэша запланирован через {ttl} секунд")
        
        await asyncio.sleep(ttl)
        
        success = await clear_cache()
        if success:
            logger.info("Кэш успешно сброшен по расписанию в 14:11")
        else:
            logger.error("Не удалось сбросить кэш по расписанию")
        
        # Чтобы избежать повторного срабатывания
        await asyncio.sleep(2)
