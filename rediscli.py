import redis

__cache = redis.Redis(host='redis', port=6379)

def get_cache():
    return __cache