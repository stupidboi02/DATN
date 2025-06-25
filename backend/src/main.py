import math
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine
from typing import List, Optional
import logging
import time
from sqlalchemy import func
from .models import Base, Segment, SegmentBase, SegmentResponse, SegmentRule, SegmentRuleBase, SegmentListResponse, UserSegmentMembership, UserProfile, PaginatedUserProfiles, UserAnalytics, AnalyticsResponse
from datetime import date, datetime
from sqlalchemy.orm import Session
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

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
    client.server_info()
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
logger.info("Ket noi successful Postgre")

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Khởi tạo scheduler
# scheduler = AsyncIOScheduler()

# async def update_all_segments(db: Session):
#     logger.info("Running scheduled segment update")
#     try:
#         active_segments = db.query(Segment).filter(Segment.is_active == True).all()
#         for segment in active_segments:
#             rules = db.query(SegmentRule).filter(SegmentRule.segment_id == segment.segment_id).all()
#             assign_users_to_segment(db, segment.segment_id, rules)
#         logger.info("Completed segment update")
#     except Exception as e:
#         logger.error(f"Error updating segments: {str(e)}")

# Start scheduler when app starts
# @app.on_event("startup")
# async def startup_event():
#     db = next(get_db())  # Get a database session
#     scheduler.add_job(update_all_segments, trigger=CronTrigger(hour=18, minute=0),args=[db], id='daily_segment_update', replace_existing=True)  # Run every 60 minutes
#     scheduler.start()
#     logger.info("Scheduler started")

# @app.on_event("shutdown")
# async def shutdown_event():
#     logger.info("shutdowning Scheduler...")
#     scheduler.shutdown()
#     logger.info("Scheduler shutdown.")

# @app.get("/scheduler/jobs")
# async def list_jobs():
#     """List all scheduled jobs."""
#     jobs = scheduler.get_jobs()
#     return [
#         {
#             "id": job.id,
#             "name": job.name,
#             "func": str(job.func),
#             "trigger": str(job.trigger),
#             "next_run_time": str(job.next_run_time)
#         }
#         for job in jobs
#     ]

# Helper function to assign users to a segment based on rules
def assign_users_to_segment(db: Session, segment_id: int, rules: List[SegmentRule], batch_size: int = 1000):
    # check if segment is not active => return
    segment = db.query(Segment).filter(Segment.segment_id == segment_id).first()
    if not segment:
        logger.error(f"Segment with ID {segment_id} not found")
        raise HTTPException(status_code=404, detail="Segment not found")
    if not segment.is_active:
        logger.info(f"Segment_id={segment_id} is not active, skipping user assignment")
        return
    
    try:
        logger.info(f"Assigning users to segment_id={segment_id}")
        if not rules:
            logger.info(f"No rules for segment_id={segment_id}, skipping assignment")
            return
        date_fields = {
            "first_visit_timestamp",
            "last_visit_timestamp",
            "last_purchase_date",
            "last_active_date",
            "update_day",
            "last_support_interaction_time"
        }
        # 1. Chuyển rule thành query MongoDB
        mongo_query = {}
        or_conditions = []
        for rule in rules:
            try:
                if rule.field in date_fields:
                    value = datetime.strptime(rule.value, "%Y-%m-%d")
                else:
                    value = float(rule.value)
                # maping toán tử
                operator_mapping = {}
                if rule.operator == "=":
                    operator_mapping = {"$eq": value}
                elif rule.operator == ">=":
                    operator_mapping = {"$gte": value}
                elif rule.operator == "<=":
                    operator_mapping = {"$lte": value}
                elif rule.operator == ">":
                    operator_mapping = {"$gt": value}
                elif rule.operator == "<":
                    operator_mapping = {"$lt": value}
                elif rule.operator == "!=":
                    operator_mapping = {"$ne": value}
                else:
                    operator_mapping = None # Hoặc dictionary rỗng, hoặc raise lỗi

                if operator_mapping is None:
                    logger.warning(f"Invalid operator for rule field={rule.field}, operator={rule.operator}")
                    continue

                condition = {rule.field: operator_mapping}

                # Apply logic: AND or OR
                if rule.logic == "OR":
                    or_conditions.append(condition)
                else:  # Default to AND
                    mongo_query.update(condition)
            except ValueError:
                logger.warning(f"Invalid rule value for field={rule.field}, value={rule.value}")
                continue

        # Combine OR conditions if any
        if or_conditions:
            if mongo_query:
                # Combine AND conditions with OR conditions
                mongo_query = {"$and": [mongo_query, {"$or": or_conditions}]}
            else:
                # Only OR conditions
                mongo_query = {"$or": or_conditions}

        # Clear existing memberships for this segment
        db.query(UserSegmentMembership).filter(UserSegmentMembership.segment_id == segment_id).delete()
        db.commit()

        # Stream user_ids from MongoDB
        cursor = collection.find(mongo_query, {"user_id": 1}).batch_size(batch_size)
        user_ids_batch = []
        total_assigned = 0

        for user in cursor:
            user_id = str(user["user_id"])
            user_ids_batch.append(user_id)
            if len(user_ids_batch) >= batch_size:
                # Check for existing memberships
                existing = set(
                    r[0] for r in db.query(UserSegmentMembership.user_id)
                    .filter(
                        UserSegmentMembership.segment_id == segment_id,
                        UserSegmentMembership.user_id.in_(user_ids_batch)
                    ).all()
                )
                # Create new memberships
                new_members = [
                    UserSegmentMembership(user_id=uid, segment_id=segment_id)
                    for uid in user_ids_batch if uid not in existing
                ]
                if new_members:
                    db.bulk_save_objects(new_members)
                    db.commit()
                total_assigned += len(new_members)
                user_ids_batch = []

        # Process remaining batch
        if user_ids_batch:
            existing = set(
                r[0] for r in db.query(UserSegmentMembership.user_id)
                .filter(
                    UserSegmentMembership.segment_id == segment_id,
                    UserSegmentMembership.user_id.in_(user_ids_batch)
                ).all()
            )
            new_members = [
                UserSegmentMembership(user_id=uid, segment_id=segment_id)
                for uid in user_ids_batch if uid not in existing
            ]
            if new_members:
                db.bulk_save_objects(new_members)
                db.commit()
            total_assigned += len(new_members)

        logger.info(f"Assigned {total_assigned} users to segment_id={segment_id}")
    except Exception as e:
        logger.error(f"Error assigning users to segment_id={segment_id}: {str(e)}")
        db.rollback()
        raise

@app.get("/user-profiles")
async def get_user_profiles(
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
    segment_id: Optional[List[int]] = Query(None, description="Filter by segment IDs"),
    db: Session = Depends(get_db)
):
    try:
        start_time = time.time()
        logger.info(f"Fetching user profiles: page={page}, size={size}, segment_id={segment_id}")

        skip = (page - 1) * size
        projection = {
            "user_id": 1, "first_visit_timestamp": 1, "last_visit_timestamp": 1,
            "last_purchase_date": 1, "last_active_date": 1, "total_visits": 1,
            "purchase_history": 1, "total_items_purchased": 1, "total_spend": 1,
            "update_day": 1, "segments": 1, "churn_risk": 1,
            "category_preferences": 1, "brand_preferences": 1,
            "total_support_interactions": 1, "avg_satisfaction_score": 1, "_id": 1
        }

        profiles = []
        totalSize = 0
        totalPage = 0

        if segment_id:
            memberships_query = db.query(UserSegmentMembership.user_id).filter(
                UserSegmentMembership.segment_id.in_(segment_id)
            )
            totalSize = memberships_query.count()
            user_ids = [int(m[0]) for m in memberships_query.offset(skip).limit(size).all() if str(m[0]).isdigit()]
            totalPage = (totalSize + size - 1) // size

            if user_ids:
                # Lấy mapping user_id -> tất cả segment_id mà user thuộc về
                memberships = db.query(UserSegmentMembership.user_id, UserSegmentMembership.segment_id).filter(
                    UserSegmentMembership.user_id.in_(user_ids)
                ).all()
                user_segments_map = {}
                for uid, segid in memberships:
                    user_segments_map.setdefault(int(uid), []).append(segid)

                query = {"user_id": {"$in": user_ids}}
                cursor = collection.find(query, projection)
                for profile in cursor:
                    try:
                        profile["id"] = str(profile["_id"])
                        del profile["_id"]
                        # Gán segments_list cho từng user
                        profile["segments_list"] = user_segments_map.get(profile["user_id"], [])
                        for k, v in profile.items():
                            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                                profile[k] = None
                        user_profile = UserProfile(**profile)
                        profiles.append(user_profile.model_dump())
                    except Exception as e:
                        logger.error(f"Error processing profile {profile.get('user_id')}: {str(e)}")
                        continue
        else:
            # Nếu không truyền segment_id, trả về toàn bộ user (phân trang trên MongoDB)
            totalSize = collection.count_documents({})
            totalPage = (totalSize + size - 1) // size
            cursor = collection.find({}, projection).skip(skip).limit(size)
            # Lấy user_id của trang hiện tại
            user_ids_in_page = []
            profiles_temp = []
            for profile in cursor:
                try:
                    profile["id"] = str(profile["_id"])
                    del profile["_id"]
                    user_ids_in_page.append(profile["user_id"])
                    profiles_temp.append(profile)
                except Exception as e:
                    logger.error(f"Error processing profile {profile.get('user_id')}: {str(e)}")
                    continue
            # Lấy mapping user_id -> tất cả segment_id mà user thuộc về
            user_segments_map = {}
            if user_ids_in_page:
                memberships = db.query(UserSegmentMembership.user_id, UserSegmentMembership.segment_id).filter(
                    UserSegmentMembership.user_id.in_(user_ids_in_page)
                ).all()
                for uid, segid in memberships:
                    user_segments_map.setdefault(int(uid), []).append(segid)
            # Gán segments_list cho từng user
            for profile in profiles_temp:
                profile["segments_list"] = user_segments_map.get(profile["user_id"], [])
                # Convert all NaN/Infinity values to None for JSON compliance
                for k, v in profile.items():
                    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                        profile[k] = None
                user_profile = UserProfile(**profile)
                profiles.append(user_profile.model_dump())

        execution_time = time.time() - start_time
        logger.info(f"Successfully fetched {len(profiles)} profiles in {execution_time:.3f} seconds")
        if not profiles:
            logger.warning("No profiles found matching the criteria")
        return PaginatedUserProfiles(
            page=page, size=size, totalPage=totalPage, totalSize=totalSize, data=profiles
        )
    except Exception as e:
        logger.error(f"Error in get_user_profiles: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch profiles: {str(e)}")

@app.get("/user-profile/{user_id}")
async def get_user_profile(user_id: int, db: Session = Depends(get_db)):
    try:
        start_time = time.time()
        logger.info(f"Fetching profile for user_id: {user_id}")
        projection = {
            "user_id": 1, "first_visit_timestamp": 1, "last_visit_timestamp": 1,
            "last_purchase_date": 1, "last_active_date": 1, "total_visits": 1,
            "purchase_history": 1, "total_items_purchased": 1, "total_spend": 1,
            "update_day": 1, "segments": 1, "churn_risk": 1,
            "category_preferences": 1, "brand_preferences": 1,
            "total_support_interactions": 1, "avg_satisfaction_score": 1, "_id": 1
        }
        profile = collection.find_one({"user_id": user_id}, projection)
        if profile:
            profile["id"] = str(profile["_id"])
            del profile["_id"]

            # --- Đồng bộ segments_list từ bảng user_segment_membership ---
            memberships = db.query(UserSegmentMembership.segment_id).filter(
                UserSegmentMembership.user_id == user_id
            ).all()
            profile["segments_list"] = [segid for (segid,) in memberships]

            # Convert all NaN/Infinity values to None for JSON compliance
            for k, v in profile.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    profile[k] = None

            user_profile = UserProfile(**profile)
            execution_time = time.time() - start_time
            logger.info(f"Successfully fetched profile for user_id: {user_id} in {execution_time:.3f} seconds")
            return user_profile
        logger.warning(f"No profile found for user_id: {user_id}")
        raise HTTPException(status_code=404, detail="User profile not found")
    except Exception as e:
        logger.error(f"Error in get_user_profile: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch profile: {str(e)}")

@app.get("/analytics/{user_id}", response_model=List[AnalyticsResponse])
async def get_analytics(
    user_id: int,
    start_date: Optional[date] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[date] = Query(None, description="End date in YYYY-MM-DD format"),
    db: Session = Depends(get_db)
):
    try:
        if (start_date is not None and end_date is None) or (start_date is None and end_date is not None):
            raise HTTPException(status_code=400, detail="Both start_date and end_date must be specified together or neither")
        if start_date is None:
            earliest_date = db.query(func.min(UserAnalytics.date)).filter(UserAnalytics.user_id == user_id).scalar()
            if earliest_date is None:
                raise HTTPException(status_code=404, detail="No data found for user")
            start_date = earliest_date
        end_date = end_date or date.today()
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="start_date must be earlier than or equal to end_date")
        if end_date > date.today():
            raise HTTPException(status_code=400, detail="end_date cannot be in the future")
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


@app.get("/segments/list", response_model=List[SegmentListResponse])
async def get_segment_list(db: Session = Depends(get_db)):
    try:
        logger.info("Fetching segment list")
        segments = db.query(Segment).all()
        return segments
    except Exception as e:
        logger.error(f"Error fetching segment list: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch segment list: {str(e)}")

@app.get("/segments/{segment_id}", response_model=SegmentResponse)
async def get_segment(segment_id: int, db: Session = Depends(get_db)):
    try:
        logger.info(f"Fetching segment: {segment_id}")
        segment = db.query(Segment).filter(Segment.segment_id == segment_id).first()
        if not segment:
            raise HTTPException(status_code=404, detail="Segment not found")
        
        rules = db.query(SegmentRule).filter(SegmentRule.segment_id == segment_id).all()
        response = SegmentResponse(
            segment_id=segment.segment_id,
            segment_name=segment.segment_name,
            description=segment.description,
            is_active=segment.is_active,
            created_at=segment.created_at,
            rules=[SegmentRuleBase(**rule.__dict__) for rule in rules]
        )
        return response
    except Exception as e:
        logger.error(f"Error fetching segment {segment_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch segment: {str(e)}")

# Segment Management Endpoints
@app.post("/segments", response_model=SegmentResponse)
async def create_segment(segment: SegmentBase, db: Session = Depends(get_db)):
    try:
        logger.info(f"Creating segment: {segment.segment_name}")
        # Check if segment_name exists
        existing_segment = db.query(Segment).filter(Segment.segment_name == segment.segment_name).first()
        if existing_segment:
            raise HTTPException(status_code=400, detail=f"Segment '{segment.segment_name}' already exists")
        
        # Create segment
        db_segment = Segment(
            segment_name=segment.segment_name,
            description=segment.description,
            is_active=segment.is_active
        )
        db.add(db_segment)
        db.commit()
        db.refresh(db_segment)

        # Create rules
        list_rules=[]
        for rule in segment.rules or []:
            db_rule = SegmentRule(
                segment_id=db_segment.segment_id,
                field=rule.field,
                operator=rule.operator,
                value=rule.value,
                logic=rule.logic
            )
            db.add(db_rule)
            list_rules.append(db_rule)
        db.commit()

        # check status active
        if db_segment.is_active:
            assign_users_to_segment(db, db_segment.segment_id, list_rules)
        else:
            logger.info(f"Segment_id={db_segment.segment_id} is not active, skipping user assignment")

        # Fetch rules for response
        rules = db.query(SegmentRule).filter(SegmentRule.segment_id == db_segment.segment_id).all()
        response = SegmentResponse(
            segment_id=db_segment.segment_id,
            segment_name=db_segment.segment_name,
            description=db_segment.description,
            is_active=db_segment.is_active,
            created_at=db_segment.created_at,
            rules=[SegmentRuleBase(**rule.__dict__) for rule in rules]
        )
        return response
    except Exception as e:
        logger.error(f"Error creating segment: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create segment: {str(e)}")
    
@app.put("/segments/{segment_id}", response_model=SegmentResponse)
async def update_segment(segment_id: int, segment: SegmentBase, db: Session = Depends(get_db)):
    try:
        logger.info(f"Updating segment: {segment_id}")
        db_segment = db.query(Segment).filter(Segment.segment_id == segment_id).first()
        if not db_segment:
            raise HTTPException(status_code=404, detail="Segment not found")

        # Check if segment_name is taken by another segment
        existing_segment = db.query(Segment).filter(
            Segment.segment_name == segment.segment_name,
            Segment.segment_id != segment_id
        ).first()
        if existing_segment:
            raise HTTPException(status_code=400, detail=f"Segment name '{segment.segment_name}' already exists")

        # Validate rules
        for rule in segment.rules or []:
            if not rule.field or not rule.operator or not rule.value:
                raise HTTPException(status_code=422, detail="All rule fields (field, operator, value) must be non-empty")

        # Update segment
        db_segment.segment_name = segment.segment_name
        db_segment.description = segment.description
        db_segment.is_active = segment.is_active

        # Delete existing rules
        db.query(SegmentRule).filter(SegmentRule.segment_id == segment_id).delete()

        # Create new rules
        new_rules = []
        for rule in segment.rules or []:
            db_rule = SegmentRule(
                segment_id=segment_id,
                field=rule.field,
                operator=rule.operator,
                value=rule.value,
                logic=rule.logic
            )
            db.add(db_rule)
            new_rules.append(db_rule)

        db.commit()
        db.refresh(db_segment)

        # --- Gán lại user vào segment dựa trên rule mới ---
        # nếu active thì gán, không thì xóa
        if db_segment.is_active:
            assign_users_to_segment(db, segment_id, new_rules)
        else:
            logger.info(f"Segment_id={segment_id} is not active, clearing user assignments")
            db.query(UserSegmentMembership).filter(UserSegmentMembership.segment_id == segment_id).delete()
            db.commit()
            
        # Fetch updated rules for response
        rules = db.query(SegmentRule).filter(SegmentRule.segment_id == segment_id).all()

        response = SegmentResponse(
            segment_id=db_segment.segment_id,
            segment_name=db_segment.segment_name,
            description=db_segment.description,
            is_active=db_segment.is_active,
            created_at=db_segment.created_at,
            rules=[SegmentRuleBase(**rule.__dict__) for rule in rules]
        )
        return response
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating segment: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to update segment: {str(e)}")

@app.delete("/segments/{segment_id}")
async def delete_segment(segment_id: int, db: Session = Depends(get_db)):
    try:
        logger.info(f"Attempting to delete segment with ID: {segment_id}")
        db_segment = db.query(Segment).filter(Segment.segment_id == segment_id).first()
        if not db_segment:
            logger.warning(f"Segment with ID {segment_id} not found")
            raise HTTPException(status_code=404, detail="Segment not found")

        # Delete the segment (related SegmentRule and UserSegmentMembership records should be deleted via CASCADE)
        db.delete(db_segment)
        db.commit()
        logger.info(f"Successfully deleted segment with ID: {segment_id}")
        return {"detail": "Segment deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting segment with ID {segment_id}: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to delete segment: {str(e)}")

# Create database tables
Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)