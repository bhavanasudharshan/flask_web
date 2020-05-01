# flask_web/app.py
# from db.mongo_db import conn
from pymongo import MongoClient
from flask import Flask
import redis
import time

app = Flask(__name__)



# redis_host = "localhost"
# redis_port = 6379
# redis_password = ""
cache =  redis.Redis(host='redis',port=6379)


@app.route('/')
def hello_world():
    return 'Hey, we have Flask in a Docker container!'


@app.route('/testredis')
def redis_test():
    print("******************************")
    # return "not workin from docker"
    retries = 5
    while True:
        try:
            return cache.incr('hits')
        except redis.exceptions.ConnectionError as exc:
            if retries == 0:
                raise exc
            retries -= 1
            time.sleep(0.5)


@app.route('/testconn')
def test_conn():
    db_client=MongoClient(host="mongodb")
    var = db_client["crm"]
    var.test.insert({'blah':'blah'})
    return var.name


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
