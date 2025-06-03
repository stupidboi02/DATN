from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from bson import ObjectId
from datetime import datetime
from typing import List, Optional
from pydantic import GetCoreSchemaHandler
from pydantic import GetJsonSchemaHandler
from pydantic import PlainSerializer
from pydantic_core import core_schema
from bson import ObjectId


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
