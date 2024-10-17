import redis.asyncio as aioredis
import config

token_blocklist = aioredis.from_url(config.broker_url)  # Correct the variable name
