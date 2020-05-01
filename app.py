# flask_web/app.py
from db.mongo_db import conn
from flask import Flask
import redis
import json

app = Flask(__name__)

redis_host = "localhost"
redis_port = 6379
redis_password = ""

@app.route('/')
def hello_world():
    return 'Hey, we have Flask in a Docker container!'

@app.route('/testredis')
def redis_test():
    try:

        # The decode_repsonses flag here directs the client to convert the responses from Redis into Python strings
        # using the default encoding utf-8.  This is client specific.
        r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)

        # step 4: Set the hello message in Redis
        r.set("msg:hello", "Hello Redis!!!")

        # step 5: Retrieve the hello message from Redis
        msg = r.get("msg:hello")
        return json.dumps(msg)

    except Exception as e:
        print(e)



@app.route('/testconn')
def test_conn():
    return conn.name


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
