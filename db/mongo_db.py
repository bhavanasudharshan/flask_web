from pymongo import MongoClient


class MongoDb:
    def __init__(self, db_name):
        self.db_client = MongoClient()
        self.db = None
        self.db_name = db_name

    def connect(self):
        if not self.db:
            self.db = self.db_client[self.db_name]
            print("DB CONNECTED!!!")
            return self.db
        return self.db

    def get_db(self):
        return self.connect()


conn = MongoDb("crm").get_db()
