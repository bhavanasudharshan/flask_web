# flask_web/app.py
from db.mongo_db import conn
from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hey, we have Flask in a Docker container!'


@app.route('/testconn')
def test_conn():
    return conn.name


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
