import os
import pymongo

from pymongo import MongoClient


class MongoDBActor:
    def __init__(self, collection_name, db_name=None):
        self.collection_name = collection_name
        self.db_name = db_name or "username_squat"
        self.connection_string = "mongodb://{}:{}@{}:{}/?authSource={}".format(
            os.environ['MONGO_USER_NAME'],
            os.environ['MONGO_PASSWORD'],
            os.environ['MONGO_IP'],
            os.environ['MONGO_PORT'],
            self.db_name
        )
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.db_name]
        self.col_name = self.db[self.collection_name]

    def insert_data(self, data):
        _insert = self.col_name.insert_one(data)
        return _insert.inserted_id

    # note this does not store the key, it will replace with new data
    def replace_insert_if_not_found(self, key, data, _upsert=True):
        replace = self.col_name.replace_one(key, data, upsert=_upsert)
        return replace.upserted_id

    # this will store the key, and will have a key as well in data
    def find_and_modify(self, key, data):
        update = self.col_name.update_one(key, {'$set': data}, upsert=True)
        return update.upserted_id

    def find_one(self, param, sort_by=None):
        if sort_by:
            return self.col_name.find_one(param, sort=[(sort_by, pymongo.DESCENDING)])
        else:
            return self.col_name.find_one(param)

    def distinct(self, key, filter=None):
        if filter:
            _found = self.col_name.distinct(key=key, filter=filter)
        else:
            _found = self.col_name.distinct(key=key)
        if None in _found:
            _found.remove(None)
        if "" in _found:
            _found.remove("")
        return _found

    def find(self, key=None, limit=None):
        if key and limit is None:
            found = self.col_name.find(key)
        elif key is None and limit:
            found = self.col_name.find({}).limit(limit)
        elif key and limit:
            found = self.col_name.find(key).limit(limit)
        elif key is None and limit is None:
            found = self.col_name.find({})
        else:
            raise Exception("query not supported")
        return found
