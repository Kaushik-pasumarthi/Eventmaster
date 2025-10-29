#!/usr/bin/env python3
"""
Fix date formats in existing database records
Converts all dates to YYYY-MM-DD format
"""
from database import SessionLocal
from models import CorporateAction
from datetime import datetime

def parse_and_fix_date(date_str):
    """Parse various date formats and return YYYY-MM-DD"""
    if not date_str or date_str == 'N.A.' or date_str == '':
        return None
    
    # Already in correct format
    if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
        try:
            # Validate it's a real date
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        except:
            pass
    
    # Try different formats
    formats = [
        '%d %b %Y',   # "17 Oct 2025"
        '%Y-%m-%d',   # "2025-10-17"
        '%d-%m-%Y',   # "17-10-2025"
        '%d/%m/%Y',   # "17/10/2025"
        '%d-%b-%Y',   # "17-Oct-2025"
        '%Y-%m-%d %H:%M:%S',  # "2025-10-17 00:00:00"
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    print(f"  ⚠️  Could not parse: {date_str}")
    return date_str  # Return as-is if can't parse

def fix_database_dates():
    """Fix all date formats in database"""
    print("\n" + "=" * 70)
    print("  DATE FORMAT FIXER")
    print("=" * 70)
    
    db = SessionLocal()
    try:
        # Get all records
        records = db.query(CorporateAction).all()
        print(f"\nFound {len(records)} records to check")
        
        fixed_count = 0
        
        for record in records:
            updated = False
            
            # Fix announcement_date
            if record.announcement_date:
                fixed_date = parse_and_fix_date(record.announcement_date)
                if fixed_date != record.announcement_date:
                    print(f"  Fixing announcement_date: {record.announcement_date} -> {fixed_date}")
                    record.announcement_date = fixed_date
                    updated = True
            
            # Fix ex_date
            if record.ex_date:
                fixed_date = parse_and_fix_date(record.ex_date)
                if fixed_date != record.ex_date:
                    print(f"  Fixing ex_date: {record.ex_date} -> {fixed_date}")
                    record.ex_date = fixed_date
                    updated = True
            
            # Fix record_date
            if record.record_date:
                fixed_date = parse_and_fix_date(record.record_date)
                if fixed_date != record.record_date:
                    print(f"  Fixing record_date: {record.record_date} -> {fixed_date}")
                    record.record_date = fixed_date
                    updated = True
            
            # Fix final_date
            if record.final_date:
                fixed_date = parse_and_fix_date(record.final_date)
                if fixed_date != record.final_date:
                    print(f"  Fixing final_date: {record.final_date} -> {fixed_date}")
                    record.final_date = fixed_date
                    updated = True
            
            if updated:
                fixed_count += 1
        
        # Commit changes
        if fixed_count > 0:
            db.commit()
            print(f"\n✅ Fixed {fixed_count} records")
        else:
            print("\n✅ All dates are already in correct format (YYYY-MM-DD)")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        db.rollback()
    finally:
        db.close()
    
    print("=" * 70)

if __name__ == "__main__":
    fix_database_dates()
