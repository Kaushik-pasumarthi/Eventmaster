from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
import os
import subprocess
from datetime import datetime, timedelta

# Import database and models
from database import get_db, engine
from models import CorporateAction, Base
import schemas

# Create tables (in production, use Alembic migrations)
# Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Prowess Corporate Actions API",
    description="Corporate Actions data from CMIE Prowess (PostgreSQL)",
    version="2.0.0",
)

# Allow all origins by default (matches README_API.md). Change as needed.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files if available (same behavior as main.py)
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
def root():
    # Prefer the static UI if available, otherwise point to API docs
    if os.path.exists("static/index.html"):
        return RedirectResponse(url="/static/index.html")
    return RedirectResponse(url="/docs")


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    # Return 204 if no favicon exists to avoid 404 noise
    if os.path.exists("static/favicon.ico"):
        return RedirectResponse(url="/static/favicon.ico")
    return RedirectResponse(url="/docs")


### Corporate Actions Endpoints (PostgreSQL + SQLAlchemy) ###


@app.get("/api/v1/corporate-actions", response_model=schemas.CorporateActionsResponse)
def get_corporate_actions(
    company: Optional[str] = Query(None, description="Filter by company name (partial match)"),
    action_type: Optional[str] = Query(None, description="Filter by action type: 'bonus' or 'dividend'"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records"),
    show_all: bool = Query(False, description="If true, do not filter by ex_date (useful for debugging)"),
    db: Session = Depends(get_db)
):
    """Get corporate actions with optional filters"""
    try:
        # Build query
        query = db.query(CorporateAction)
        
        # Date filter (unless show_all is True)
        if not show_all:
            today = datetime.now().date().isoformat()
            query = query.filter(CorporateAction.ex_date >= today)
        
        # Company filter
        if company:
            query = query.filter(CorporateAction.company_name.ilike(f"%{company}%"))
        
        # Action type filter
        if action_type:
            query = query.filter(CorporateAction.action_type == action_type.lower())
        
        # Order and limit
        query = query.order_by(CorporateAction.ex_date.asc()).limit(limit)
        
        # Execute query
        results = query.all()
        
        # Convert to dictionaries
        records = []
        for record in results:
            record_dict = {
                "id": record.id,
                "company_name": record.company_name,
                "action_type": record.action_type,
                "announcement_date": record.announcement_date,
                "ex_date": record.ex_date,
                "record_date": record.record_date,
                "final_date": record.final_date,
                "dividend_rate": record.dividend_rate,
                "dividend_type": record.dividend_type,
                "ratio_numerator": record.ratio_numerator,
                "ratio_denominator": record.ratio_denominator,
                "security_type": record.security_type,
                # New fields from Alfago API
                "security_id": record.security_id,
                "market_code": record.market_code,
                "symbol": record.symbol,
                "isin": record.isin,
                # Split fields
                "old_face_value": record.old_face_value,
                "new_face_value": record.new_face_value,
                "split_ratio": record.split_ratio,
                # Rights fields
                "rights_ratio_numerator": record.rights_ratio_numerator,
                "rights_ratio_denominator": record.rights_ratio_denominator,
                "rights_price": record.rights_price,
                "created_at": record.created_at.isoformat() if record.created_at else None
            }
            records.append(record_dict)
        
        return {
            "status": "success",
            "count": len(records),
            "data": records,
            "metadata": {
                "filters_applied": {"company": company, "action_type": action_type, "limit": limit},
                "query_time": datetime.utcnow().isoformat(),
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/api/v1/corporate-actions/upcoming")
def get_upcoming_actions(
    days_ahead: int = Query(30, ge=1, le=365, description="Days to look ahead"),
    action_type: Optional[str] = Query(None, description="Filter by action type"),
    db: Session = Depends(get_db)
):
    """Get upcoming corporate actions (final_date >= today) within specified days"""
    try:
        today = datetime.now().date().isoformat()
        future_date = (datetime.now().date() + timedelta(days=days_ahead)).isoformat()
        
        # Build query - using final_date for upcoming events
        query = db.query(CorporateAction).filter(
            and_(
                CorporateAction.final_date >= today,
                CorporateAction.final_date <= future_date
            )
        )
        
        # Action type filter
        if action_type:
            query = query.filter(CorporateAction.action_type == action_type.lower())
        
        # Order by final_date
        query = query.order_by(CorporateAction.final_date.asc())
        
        # Execute query
        results = query.all()
        
        # Convert to dictionaries with all fields
        records = []
        for record in results:
            record_dict = {
                "id": record.id,
                "company_name": record.company_name,
                "action_type": record.action_type,
                "market_code": record.market_code,
                "security_id": record.security_id,
                "symbol": record.symbol,
                "isin": record.isin,
                "announcement_date": record.announcement_date,
                "ex_date": record.ex_date,
                "record_date": record.record_date,
                "final_date": record.final_date,
                "dividend_rate": record.dividend_rate,
                "dividend_type": record.dividend_type,
                "ratio_numerator": record.ratio_numerator,
                "ratio_denominator": record.ratio_denominator,
                "old_face_value": record.old_face_value,
                "new_face_value": record.new_face_value,
                "split_ratio": record.split_ratio,
                "rights_ratio_numerator": record.rights_ratio_numerator,
                "rights_ratio_denominator": record.rights_ratio_denominator,
                "rights_price": record.rights_price,
                "security_type": record.security_type,
            }
            records.append(record_dict)

        return {
            "status": "success",
            "count": len(records),
            "upcoming_actions": records,
            "days_ahead": days_ahead,
            "date_range": {"from": today, "to": future_date},
            "query_time": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/v1/corporate-actions/today")
def get_today_actions(
    action_type: Optional[str] = Query(None, description="Filter by action type"),
    market_code: Optional[str] = Query(None, description="Filter by market: NSE or BSE"),
    db: Session = Depends(get_db)
):
    """Get corporate actions with final_date = today (for immediate action)"""
    try:
        today = datetime.now().date().isoformat()
        
        # Build query - final_date equals today
        query = db.query(CorporateAction).filter(
            CorporateAction.final_date == today
        )
        
        # Action type filter
        if action_type:
            query = query.filter(CorporateAction.action_type == action_type.lower())
        
        # Market filter
        if market_code:
            query = query.filter(CorporateAction.market_code == market_code.upper())
        
        # Order by company name
        query = query.order_by(CorporateAction.company_name.asc())
        
        # Execute query
        results = query.all()
        
        # Convert to dictionaries with all fields
        records = []
        for record in results:
            record_dict = {
                "id": record.id,
                "company_name": record.company_name,
                "action_type": record.action_type,
                "market_code": record.market_code,
                "security_id": record.security_id,
                "symbol": record.symbol,
                "isin": record.isin,
                "announcement_date": record.announcement_date,
                "ex_date": record.ex_date,
                "record_date": record.record_date,
                "final_date": record.final_date,
                "dividend_rate": record.dividend_rate,
                "dividend_type": record.dividend_type,
                "ratio_numerator": record.ratio_numerator,
                "ratio_denominator": record.ratio_denominator,
                "old_face_value": record.old_face_value,
                "new_face_value": record.new_face_value,
                "split_ratio": record.split_ratio,
                "rights_ratio_numerator": record.rights_ratio_numerator,
                "rights_ratio_denominator": record.rights_ratio_denominator,
                "rights_price": record.rights_price,
                "security_type": record.security_type,
            }
            records.append(record_dict)

        return {
            "status": "success",
            "count": len(records),
            "date": today,
            "actions_today": records,
            "filters_applied": {"action_type": action_type, "market_code": market_code},
            "query_time": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/v1/corporate-actions/dividends")
def get_dividends(
    company: Optional[str] = Query(None, description="Filter by company name"),
    min_rate: Optional[float] = Query(None, description="Minimum dividend rate"),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """Get dividend corporate actions"""
    try:
        today = datetime.now().date().isoformat()
        
        # Build query for dividends only
        query = db.query(CorporateAction).filter(
            and_(
                CorporateAction.action_type == 'dividend',
                CorporateAction.ex_date >= today
            )
        )
        
        # Company filter
        if company:
            query = query.filter(CorporateAction.company_name.ilike(f"%{company}%"))
        
        # Minimum rate filter
        if min_rate:
            query = query.filter(CorporateAction.dividend_rate >= min_rate)
        
        # Order and limit
        query = query.order_by(CorporateAction.ex_date.asc()).limit(limit)
        
        # Execute query
        results = query.all()
        
        # Convert to dictionaries
        records = []
        for record in results:
            record_dict = {
                "company_name": record.company_name,
                "announcement_date": record.announcement_date,
                "ex_date": record.ex_date,
                "dividend_rate": record.dividend_rate,
                "dividend_type": record.dividend_type,
            }
            records.append(record_dict)

        return {
            "status": "success",
            "dividend_count": len(records),
            "dividends": records,
            "filters": {"company": company, "min_rate": min_rate},
            "query_time": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/v1/corporate-actions/bonus")
def get_bonus_issues(
    company: Optional[str] = Query(None, description="Filter by company name"),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """Get bonus issue corporate actions"""
    try:
        today = datetime.now().date().isoformat()
        
        # Build query for bonus issues only
        query = db.query(CorporateAction).filter(
            and_(
                CorporateAction.action_type == 'bonus',
                CorporateAction.ex_date >= today
            )
        )
        
        # Company filter
        if company:
            query = query.filter(CorporateAction.company_name.ilike(f"%{company}%"))
        
        # Order and limit
        query = query.order_by(CorporateAction.ex_date.asc()).limit(limit)
        
        # Execute query
        results = query.all()
        
        # Convert to dictionaries
        records = []
        for record in results:
            record_dict = {
                "company_name": record.company_name,
                "announcement_date": record.announcement_date,
                "ex_date": record.ex_date,
                "ratio_numerator": record.ratio_numerator,
                "ratio_denominator": record.ratio_denominator,
                "security_type": record.security_type,
            }
            # Add ratio display
            if record.ratio_numerator and record.ratio_denominator:
                try:
                    record_dict['ratio_display'] = f"{int(record.ratio_numerator)}:{int(record.ratio_denominator)}"
                except Exception:
                    record_dict['ratio_display'] = None
            else:
                record_dict['ratio_display'] = None
            
            records.append(record_dict)

        return {
            "status": "success",
            "bonus_count": len(records),
            "bonus_issues": records,
            "filters": {"company": company},
            "query_time": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/v1/corporate-actions/company/{company_name}")
def get_company_actions(
    company_name: str,
    db: Session = Depends(get_db)
):
    """Get all upcoming corporate actions for a specific company"""
    try:
        today = datetime.now().date().isoformat()
        
        # Build query
        query = db.query(CorporateAction).filter(
            and_(
                CorporateAction.company_name.ilike(f"%{company_name}%"),
                CorporateAction.ex_date >= today
            )
        ).order_by(CorporateAction.ex_date.asc())
        
        # Execute query
        results = query.all()
        
        if not results:
            raise HTTPException(status_code=404, detail=f"No upcoming actions found for company: {company_name}")
        
        # Convert to dictionaries
        records = []
        for record in results:
            record_dict = {
                "id": record.id,
                "company_name": record.company_name,
                "action_type": record.action_type,
                "announcement_date": record.announcement_date,
                "ex_date": record.ex_date,
                "record_date": record.record_date,
                "final_date": record.final_date,
                "dividend_rate": record.dividend_rate,
                "dividend_type": record.dividend_type,
                "ratio_numerator": record.ratio_numerator,
                "ratio_denominator": record.ratio_denominator,
                "security_type": record.security_type,
            }
            records.append(record_dict)

        return {
            "status": "success",
            "company": company_name,
            "actions_count": len(records),
            "actions": records,
            "query_time": datetime.utcnow().isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.post("/api/v1/corporate-actions/refresh")
def refresh_corporate_actions():
    try:
        result = subprocess.run(['python', 'daily_updater.py'], capture_output=True, text=True, cwd='.')

        if result.returncode == 0:
            output = result.stdout
            return {
                "status": "success",
                "message": "Corporate actions data refreshed successfully",
                "refresh_time": datetime.utcnow().isoformat(),
                "details": output.split('\n')[-10:],
            }
        else:
            raise HTTPException(status_code=500, detail=f"Refresh failed: {result.stderr}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Refresh error: {str(e)}")


@app.get("/api/v1/corporate-actions/stats", response_model=schemas.StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    """Get statistics about corporate actions"""
    try:
        today = datetime.now().date().isoformat()
        week_ahead = (datetime.now().date() + timedelta(days=7)).isoformat()
        
        # Total active actions
        total_active = db.query(func.count(CorporateAction.id)).filter(
            CorporateAction.ex_date >= today
        ).scalar()
        
        # Count by action type
        by_type_query = db.query(
            CorporateAction.action_type,
            func.count(CorporateAction.id).label('count')
        ).filter(
            CorporateAction.ex_date >= today
        ).group_by(CorporateAction.action_type).all()
        
        by_type = [{"action_type": action_type, "count": count} for action_type, count in by_type_query]
        
        # Upcoming this week
        upcoming_week = db.query(func.count(CorporateAction.id)).filter(
            and_(
                CorporateAction.ex_date >= today,
                CorporateAction.ex_date <= week_ahead
            )
        ).scalar()

        return {
            "status": "success",
            "total_active_actions": total_active or 0,
            "by_type": by_type,
            "upcoming_this_week": upcoming_week or 0,
            "last_updated": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats error: {str(e)}")

