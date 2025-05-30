import redis.asyncio as redis
import os
from dotenv import load_dotenv
import asyncio
from utils.logger import logger
from typing import List, Any

# Redis client
client = None
_initialized = False
_init_lock = asyncio.Lock()

# Constants
REDIS_KEY_TTL = 3600 * 24  # 24 hour TTL as safety mechanism


def initialize():
    """Initialize Redis connection using environment variables."""
    global client

    # Load environment variables if not already loaded
    load_dotenv()

    # Get Redis configuration
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    redis_password = os.getenv('REDIS_PASSWORD', '')
    # Convert string 'True'/'False' to boolean
    redis_ssl_str = os.getenv('REDIS_SSL', 'False')
    redis_ssl = redis_ssl_str.lower() == 'true'

    logger.info(f"Initializing Redis connection to {redis_host}:{redis_port}")

    # Create Redis client with basic configuration
    client = redis.Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        ssl=redis_ssl,
        decode_responses=True,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
        retry_on_timeout=True,
        health_check_interval=30
    )

    return client


async def initialize_async():
    """Initialize Redis connection asynchronously."""
    global client, _initialized

    async with _init_lock:
        if not _initialized:
            logger.info("Initializing Redis connection")
            initialize()

            try:
                await client.ping()
                logger.info("Successfully connected to Redis")
                _initialized = True
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                client = None
                raise

    return client


async def close():
    """Close Redis connection."""
    global client, _initialized
    if client:
        logger.info("Closing Redis connection")
        await client.aclose()
        client = None
        _initialized = False
        logger.info("Redis connection closed")


async def get_client():
    """Get the Redis client, initializing if necessary."""
    global client, _initialized
    if client is None or not _initialized:
        await initialize_async()
    return client


# Basic Redis operations
async def set(key: str, value: str, ex: int = None):
    """Set a Redis key."""
    logger.debug(f"[Redis] SET key={key} ex={ex}")
    redis_client = await get_client()
    result = await redis_client.set(key, value, ex=ex)
    logger.debug(f"[Redis] SET result for key={key}: {result}")
    return result


async def get(key: str, default: str = None):
    """Get a Redis key."""
    logger.debug(f"[Redis] GET key={key}")
    redis_client = await get_client()
    result = await redis_client.get(key)
    logger.debug(f"[Redis] GET result for key={key}: {result}")
    return result if result is not None else default


async def delete(key: str):
    """Delete a Redis key."""
    logger.debug(f"[Redis] DELETE key={key}")
    redis_client = await get_client()
    result = await redis_client.delete(key)
    logger.debug(f"[Redis] DELETE result for key={key}: {result}")
    return result


async def publish(channel: str, message: str):
    """Publish a message to a Redis channel."""
    logger.debug(f"[Redis] PUBLISH channel={channel} message={message[:100]}")
    redis_client = await get_client()
    result = await redis_client.publish(channel, message)
    logger.debug(f"[Redis] PUBLISH result for channel={channel}: {result}")
    return result


async def create_pubsub():
    """Create a Redis pubsub object."""
    logger.debug(f"[Redis] Creating pubsub object")
    redis_client = await get_client()
    pubsub = redis_client.pubsub()
    logger.debug(f"[Redis] Pubsub object created")
    return pubsub


# List operations
async def rpush(key: str, *values: Any):
    """Append one or more values to a list."""
    logger.debug(f"[Redis] RPUSH key={key} values={values}")
    redis_client = await get_client()
    result = await redis_client.rpush(key, *values)
    logger.debug(f"[Redis] RPUSH result for key={key}: {result}")
    return result


async def lrange(key: str, start: int, end: int) -> List[str]:
    """Get a range of elements from a list."""
    logger.debug(f"[Redis] LRANGE key={key} start={start} end={end}")
    redis_client = await get_client()
    result = await redis_client.lrange(key, start, end)
    logger.debug(f"[Redis] LRANGE result for key={key}: {result}")
    return result


async def llen(key: str) -> int:
    """Get the length of a list."""
    logger.debug(f"[Redis] LLEN key={key}")
    redis_client = await get_client()
    result = await redis_client.llen(key)
    logger.debug(f"[Redis] LLEN result for key={key}: {result}")
    return result


# Key management
async def expire(key: str, time: int):
    """Set a key's time to live in seconds."""
    logger.debug(f"[Redis] EXPIRE key={key} time={time}")
    redis_client = await get_client()
    result = await redis_client.expire(key, time)
    logger.debug(f"[Redis] EXPIRE result for key={key}: {result}")
    return result


async def keys(pattern: str) -> List[str]:
    """Get keys matching a pattern."""
    logger.debug(f"[Redis] KEYS pattern={pattern}")
    redis_client = await get_client()
    result = await redis_client.keys(pattern)
    logger.debug(f"[Redis] KEYS result for pattern={pattern}: {result}")
    return result