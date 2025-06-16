from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from bson import ObjectId
from datetime import datetime
from typing import List, Optional
from pydantic import GetCoreSchemaHandler
from pydantic import GetJsonSchemaHandler
from pydantic_core import core_schema
from sqlalchemy import  Column, Integer, Date, DateTime, Float, PrimaryKeyConstraint, Boolean, func, Text, String, ForeignKey
from datetime import datetime, date
from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base

from bson import ObjectId

Base = declarative_base()


class PyObjectId(ObjectId):
    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
        return core_schema.no_info_plain_validator_function(cls.validate)

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid ObjectId')
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema, handler: GetJsonSchemaHandler):
        return {'type': 'string'}

class UserProfile(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id")
    user_id: int
    first_visit_timestamp: Optional[datetime]
    last_visit_timestamp: Optional[datetime]
    last_purchase_date: Optional[datetime] = None
    last_active_date: Optional[datetime]
    total_visits: int
    purchase_history: List[Any] = []
    total_items_purchased: int
    total_spend: float
    update_day: Optional[datetime]
    segments: Optional[str]
    segments_list: Optional[List[int]] = []
    churn_risk: Optional[str]
    category_preferences: Optional[Dict[str, float]]
    brand_preferences: Optional[Dict[str, float]]
    total_support_interactions: Optional[int] = 0
    total_calls: Optional[int] = 0
    total_chats: Optional[int] = 0
    total_tickets: Optional[int] = 0
    last_support_interaction_time: Optional[datetime] = None
    avg_satisfaction_score: Optional[float] = None
    most_frequent_issue_category: Optional[str] = None
    support_prone_flag: Optional[bool] = False

    class Config:
        validate_by_name = True  # Thay cho allow_population_by_field_name
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
class PaginatedUserProfiles(BaseModel):
    page: int
    size: int
    totalPage: int
    totalSize: int
    data: List[UserProfile]

# SQLAlchemy Models
class UserAnalytics(Base):
    __tablename__ = "daily_user_metrics"
    user_id = Column(Integer, index=True)
    date = Column(Date)
    daily_total_visits = Column(Float)
    daily_total_view = Column(Float)
    daily_total_add_to_cart = Column(Integer)
    daily_total_purchase = Column(Float)
    daily_total_spend = Column(Float)
    __table_args__ = (PrimaryKeyConstraint('user_id', 'date', name='prime_users_key_date'),)

class Segment(Base):
    __tablename__ = "segments"
    segment_id = Column(Integer, primary_key=True)
    segment_name = Column(String(255), nullable=False, unique=True)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    is_active = Column(Boolean, default=True)

class SegmentRule(Base):
    __tablename__ = "segment_rules"
    rule_id = Column(Integer, primary_key=True)
    segment_id = Column(Integer, ForeignKey("segments.segment_id", ondelete="CASCADE"), nullable=False)
    field = Column(String(100), nullable=False)
    operator = Column(String(20), nullable=False)
    value = Column(String(255), nullable=False)
    logic = Column(String(10))

class UserSegmentMembership(Base):
    __tablename__ = "user_segment_membership"
    user_id = Column(Integer, primary_key=True)
    segment_id = Column(Integer, ForeignKey("segments.segment_id", ondelete="CASCADE"), primary_key=True)
    assigned_at = Column(DateTime, default=datetime.now)

# Pydantic Models
class AnalyticsResponse(BaseModel):
    user_id: int
    date: date
    daily_total_visits: Optional[int] = None
    daily_total_view: Optional[int] = None
    daily_total_add_to_cart: Optional[int] = None
    daily_total_purchase: Optional[int] = None
    daily_total_spend: Optional[float] = None
    class Config:
        from_attributes = True

class SegmentRuleBase(BaseModel):
    field: str
    operator: str
    value: str
    logic: Optional[str] = None

class SegmentBase(BaseModel):
    segment_name: str
    description: Optional[str] = None
    is_active: Optional[bool] = True
    rules: Optional[List[SegmentRuleBase]] = []

class SegmentResponse(SegmentBase):
    segment_id: int
    created_at: datetime
    rules: List[SegmentRuleBase] = []
    class Config:
        from_attributes = True

class SegmentListResponse(BaseModel):
    segment_id: int
    segment_name: str
    description: Optional[str] = None
    created_at: datetime
    is_active: bool
    class Config:
        from_attributes = True

class SegmentRuleResponse(SegmentRuleBase):
    rule_id: int
    segment_id: int
    class Config:
        from_attributes = True

# class UserSegmentMembershipBase(BaseModel):
#     user_id: int
#     segment_id: int

# class UserSegmentMembershipResponse(UserSegmentMembershipBase):
#     assigned_at: datetime
#     class Config:
#         from_attributes = True