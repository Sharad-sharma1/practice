import redis

client = redis.StrictRedis(host='localhost', port=6380, db=0)
try:
    print(client.ping())  # Should return True
except redis.ConnectionError:
    print("Could not connect to Redis.")
