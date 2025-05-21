from fastapi import Depends
from pymongo import MongoClient
from .db import get_database

def get_user_profile_collection():
    db = get_database()
    return db["user_profile"]