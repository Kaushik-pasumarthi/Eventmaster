"""
SQLAlchemy models for Corporate Actions database
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()


class CorporateAction(Base):
    """
    Corporate Actions model - stores dividend, bonus, splits, and rights data for NSE/BSE
    """
    __tablename__ = "corporate_actions"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    
    # Company identification
    company_name = Column(String(500), nullable=False, index=True)
    security_id = Column(Integer, nullable=True, index=True)  # Alfago security ID
    market_code = Column(String(10), nullable=True, index=True)  # 'NSE' or 'BSE'
    symbol = Column(String(50), nullable=True)  # Stock symbol
    isin = Column(String(50), nullable=True)  # ISIN code
    
    # Action details
    action_type = Column(String(50), nullable=False, index=True)  # 'dividend', 'bonus', 'split', 'rights'
    announcement_date = Column(String(50))
    ex_date = Column(String(50), index=True)
    record_date = Column(String(50))
    final_date = Column(String(50))
    
    # Dividend specific
    dividend_rate = Column(Float, nullable=True)
    dividend_type = Column(String(100), nullable=True)
    
    # Bonus specific
    ratio_numerator = Column(Float, nullable=True)
    ratio_denominator = Column(Float, nullable=True)
    
    # Split specific
    old_face_value = Column(Float, nullable=True)
    new_face_value = Column(Float, nullable=True)
    split_ratio = Column(String(50), nullable=True)
    
    # Rights specific
    rights_ratio_numerator = Column(Integer, nullable=True)
    rights_ratio_denominator = Column(Integer, nullable=True)
    rights_price = Column(Float, nullable=True)
    
    # Common fields
    security_type = Column(String(200), nullable=True)
    raw_data = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint('company_name', 'action_type', 'market_code', 'announcement_date', 'ex_date', 
                        name='uix_company_action_market_dates'),
    )

    def __repr__(self):
        return f"<CorporateAction(id={self.id}, company={self.company_name}, type={self.action_type}, market={self.market_code}, ex_date={self.ex_date})>"
