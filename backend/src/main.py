from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient
from sqlalchemy import create_engine, Column, Integer, Date, Float, PrimaryKeyConstraint, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from bson import ObjectId
from datetime import datetime, timedelta,date
from typing import List, Optional, Dict, Any
import logging
import time
from .models import UserProfile, PaginatedUserProfiles, PyObjectId

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
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

# PostgreSQL connection
DATABASE_URL = "postgresql+psycopg2://mydatabase:mydatabase@127.0.0.1:5432/mydatabase"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
logger.info("Ket noi thanh cong Postgre")

# PostgreSQL model
class UserAnalytics(Base):
    __tablename__ = "daily_user_metrics"
    user_id = Column(Integer, index=True)
    date = Column(Date)
    daily_total_event = Column(Float)
    daily_total_visits = Column(Float)
    daily_total_view = Column(Float)
    daily_total_add_to_cart = Column(Float)
    daily_total_purchase = Column(Float)
    daily_total_spend = Column(Float)
    
    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'date', name='pk_user_date'),
    )
# Pydantic models
class AnalyticsResponse(BaseModel):
    user_id: int
    date: date
    daily_total_event: float
    daily_total_visits: float
    daily_total_view: float
    daily_total_add_to_cart: float
    daily_total_purchase: float
    daily_total_spend: float

    class Config:
        from_attributes  = True

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
    total_support_interactions: Optional[int] = None
    avg_satisfaction_score: Optional[float] = None
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
            "total_support_interactions": 1,
            "avg_satisfaction_score": 1,
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
            "total_support_interactions": 1,
            "avg_satisfaction_score": 1,
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
    
# Updated endpoint: Get analytics data with custom date range
@app.get("/analytics/{user_id}", response_model=List[AnalyticsResponse])
async def get_analytics(
    user_id: int,
    start_date: Optional[date] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[date] = Query(None, description="End date in YYYY-MM-DD format")
):
    # Validate input
    if (start_date is not None and end_date is None) or (start_date is None and end_date is not None):
        raise HTTPException(status_code=400, detail="Both start_date and end_date must be specified together or neither")

    # Database session
    db = SessionLocal()
    try:
        # Get the earliest date for the user if start_date is not provided
        if start_date is None:
            earliest_date = db.query(func.min(UserAnalytics.date)).filter(UserAnalytics.user_id == user_id).scalar()
            if earliest_date is None:
                raise HTTPException(status_code=404, detail="No data found for user")
            start_date = earliest_date
        # Use today as default end_date if not provided
        end_date = end_date or date.today()

        # Validate date range
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="start_date must be earlier than or equal to end_date")
        if end_date > date.today():
            raise HTTPException(status_code=400, detail="end_date cannot be in the future")

        # Database query
        results = db.query(UserAnalytics).filter(
            UserAnalytics.user_id == user_id,
            UserAnalytics.date >= start_date,
            UserAnalytics.date <= end_date
        ).order_by(UserAnalytics.date.asc()).all()
        
        if not results:
            raise HTTPException(status_code=404, detail="No data found for user in the specified date range")
        
        return results
    finally:
        db.close()

# Create database tables for PostgreSQL
Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)