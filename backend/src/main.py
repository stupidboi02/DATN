from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pydantic import BaseModel
from .models import UserProfile, PaginatedUserProfiles
from typing import List, Optional, Dict
from bson import ObjectId
from datetime import datetime
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
try:
    client = MongoClient("mongodb://admin:admin@127.0.0.1:27017", serverSelectionTimeoutMS=5000)
    client.server_info()  # Test connection
    logger.info("Connected to MongoDB successfully")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {str(e)}")
    raise Exception(f"MongoDB connection failed: {str(e)}")

db = client["admin"]
collection = db["user_profile"]

# Pydantic models

class UserProfile(BaseModel):
    id: str
    user_id: int
    first_visit_timestamp: datetime
    last_visit_timestamp: datetime
    last_purchase_date: Optional[datetime] = None
    last_active_date: datetime
    total_visits: int
    purchase_history: List
    total_items_purchased: int
    total_spend: int
    update_day: datetime
    segments: str
    churn_risk: str
    category_preferences: Optional[Dict]
    brand_preferences: Optional[Dict]

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat()
        }

@app.get("/user-profiles", response_model=PaginatedUserProfiles)
async def get_user_profiles(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    size: int = Query(10, ge=1, le=100, description="Number of records per page"),
    user_id: Optional[int] = Query(None, description="Filter by exact user ID"),
    min_total_visits: Optional[int] = Query(None, ge=0, description="Minimum total visits"),
    max_total_visits: Optional[int] = Query(None, ge=0, description="Maximum total visits"),
    min_total_spend: Optional[int] = Query(None, ge=0, description="Minimum total spend"),
    max_total_spend: Optional[int] = Query(None, ge=0, description="Maximum total spend"),
    min_total_items_purchased: Optional[int] = Query(None, ge=0, description="Minimum total items purchased"),
    max_total_items_purchased: Optional[int] = Query(None, ge=0, description="Maximum total items purchased"),
    segments: Optional[str] = Query(None, description="Filter by exact segments value"),
    churn_risk: Optional[str] = Query(None, description="Filter by exact churn risk value")
):
    try:
        start_time = time.time()
        logger.info(f"Fetching user profiles: page={page}, size={size}, filters={{user_id={user_id}, total_visits=({min_total_visits},{max_total_visits}), total_spend=({min_total_spend},{max_total_spend}), total_items_purchased=({min_total_items_purchased},{max_total_items_purchased}), segments={segments}, churn_risk={churn_risk}}}")

        # Build MongoDB query
        query = {}
        if user_id is not None:
            query["user_id"] = user_id
        if min_total_visits is not None or max_total_visits is not None:
            query["total_visits"] = {}
            if min_total_visits is not None:
                query["total_visits"]["$gte"] = min_total_visits
            if max_total_visits is not None:
                query["total_visits"]["$lte"] = max_total_visits
        if min_total_spend is not None or max_total_spend is not None:
            query["total_spend"] = {}
            if min_total_spend is not None:
                query["total_spend"]["$gte"] = min_total_spend
            if max_total_spend is not None:
                query["total_spend"]["$lte"] = max_total_spend
        if min_total_items_purchased is not None or max_total_items_purchased is not None:
            query["total_items_purchased"] = {}
            if min_total_items_purchased is not None:
                query["total_items_purchased"]["$gte"] = min_total_items_purchased
            if max_total_items_purchased is not None:
                query["total_items_purchased"]["$lte"] = max_total_items_purchased
        if segments is not None:
            query["segments"] = segments
        if churn_risk is not None:
            query["churn_risk"] = churn_risk

        skip = (page - 1) * size

        projection = {
            "user_id": 1,
            "first_visit_timestamp": 1,
            "last_visit_timestamp": 1,
            "last_purchase_date": 1,
            "last_active_date": 1,
            "total_visits": 1,
            "purchase_history": 1,
            "total_items_purchased": 1,
            "total_spend": 1,
            "update_day": 1,
            "segments": 1,
            "churn_risk": 1,
            "category_preferences": 1,
            "brand_preferences": 1,
            "_id": 1
        }

        totalSize = collection.count_documents(query)
        totalPage = (totalSize + size - 1) // size

        profiles = []
        try:
            cursor = collection.find(query, projection).skip(skip).limit(size)
            for profile in cursor:
                try:
                    profile["id"] = str(profile["_id"])
                    del profile["_id"]
                    user_profile = UserProfile(**profile)
                    profiles.append(user_profile.model_dump())  # <-- Sửa dòng này
                except Exception as e:
                    logger.error(f"Error processing profile {profile.get('user_id')}: {str(e)}")
                    continue
        except Exception as e:
            logger.error(f"Error executing MongoDB query: {str(e)}")
            raise HTTPException(status_code=500, detail=f"MongoDB query failed: {str(e)}")

        execution_time = time.time() - start_time
        logger.info(f"Successfully fetched {len(profiles)} profiles in {execution_time:.3f} seconds")
        if not profiles:
            logger.warning("No profiles found matching the criteria")

        return PaginatedUserProfiles(
            page=page,
            size=size,
            totalPage=totalPage,
            totalSize=totalSize,
            data=profiles  # <-- Bây giờ là list các dict
        )
    except Exception as e:
        logger.error(f"Error in get_user_profiles: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch profiles: {str(e)}")

@app.get("/user-profile/{user_id}", response_model=UserProfile)
async def get_user_profile(user_id: int):
    try:
        start_time = time.time()
        logger.info(f"Fetching profile for user_id: {user_id}")
        
        # Define projection
        projection = {
            "user_id": 1,
            "first_visit_timestamp": 1,
            "last_visit_timestamp": 1,
            "last_purchase_date": 1,
            "last_active_date": 1,
            "total_visits": 1,
            "purchase_history": 1,
            "total_items_purchased": 1,
            "total_spend": 1,
            "update_day": 1,
            "segments": 1,
            "churn_risk": 1,
            "category_preferences": 1,
            "brand_preferences": 1,
            "_id": 1
        }
        
        profile = collection.find_one({"user_id": user_id}, projection)
        if profile:
            profile["id"] = str(profile["_id"])
            del profile["_id"]
            # Validate with Pydantic explicitly
            user_profile = UserProfile(**profile)
            execution_time = time.time() - start_time
            logger.info(f"Successfully fetched profile for user_id: {user_id} in {execution_time:.3f} seconds")
            return user_profile
        logger.warning(f"No profile found for user_id: {user_id}")
        raise HTTPException(status_code=404, detail="User profile not found")
    except Exception as e:
        logger.error(f"Error in get_user_profile: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch profile: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)