from pymongo import MongoClient
from bson import ObjectId
from typing import List, Dict, Any

class MongoDB:
    def __init__(self, username: str, password: str, host: str = "localhost", port: int = 27017):
        self.client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}/")
        self.db = self.client["admin"]
        self.collection = self.db["user_profile"]

    def get_database():
        client = MongoClient("mongodb://admin:admin@localhost:27017/")
        return client["admin"]

    def get_user_profiles(self) -> List[Dict[str, Any]]:
        return list(self.collection.find())

    def get_user_profile_by_id(self, user_id: str) -> Dict[str, Any]:
        return self.collection.find_one({"_id": ObjectId(user_id)})