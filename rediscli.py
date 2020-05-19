import redis

__cache = redis.Redis(host='redis', port=6379, decode_responses=True)


def get_cache():
    return __cache
