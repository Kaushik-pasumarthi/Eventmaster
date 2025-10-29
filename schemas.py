"""
Pydantic schemas for API request/response validation
"""
from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime


class CorporateActionBase(BaseModel):
    """Base schema for Corporate Action"""
    company_name: str
    action_type: str
    announcement_date: Optional[str] = None
    ex_date: Optional[str] = None
    record_date: Optional[str] = None
    final_date: Optional[str] = None
    dividend_rate: Optional[float] = None
    dividend_type: Optional[str] = None
    ratio_numerator: Optional[float] = None
    ratio_denominator: Optional[float] = None
    security_type: Optional[str] = None
    raw_data: Optional[str] = None
    
    # New fields from Alfago API integration
    security_id: Optional[int] = None
    market_code: Optional[str] = None
    symbol: Optional[str] = None
    isin: Optional[str] = None
    
    # Split fields
    old_face_value: Optional[float] = None
    new_face_value: Optional[float] = None
    split_ratio: Optional[str] = None
    
    # Rights fields
    rights_ratio_numerator: Optional[float] = None
    rights_ratio_denominator: Optional[float] = None
    rights_price: Optional[float] = None


class CorporateActionCreate(CorporateActionBase):
    """Schema for creating a corporate action"""
    pass


class CorporateActionOut(CorporateActionBase):
    """Schema for corporate action output"""
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CorporateActionsResponse(BaseModel):
    """Response schema for list of corporate actions"""
    status: str
    count: int
    data: list[dict]
    metadata: Optional[dict] = None


class StatsResponse(BaseModel):
    """Response schema for statistics"""
    status: str
    total_active_actions: int
    by_type: list[dict]
    upcoming_this_week: int
    last_updated: str
