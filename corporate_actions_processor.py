"""
Comprehensive Corporate Actions Processor
Processes all action types from NSE and BSE batch files
"""
import json
import os
from typing import List, Dict
from datetime import datetime
from sqlalchemy.orm import Session
from database import SessionLocal
from models import CorporateAction
import alfago_client

def parse_date(date_str: str) -> str:
    """Parse date string in various formats and return standardized YYYY-MM-DD format"""
    if not date_str or date_str == 'N.A.' or date_str == '':
        return None
    
    # Try different date formats
    formats = [
        '%d %b %Y',  # "17 Oct 2025"
        '%Y-%m-%d',  # "2025-10-17"
        '%d-%m-%Y',  # "17-10-2025"
        '%d/%m/%Y',  # "17/10/2025"
        '%d-%b-%Y',  # "17-Oct-2025"
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            # Always return in YYYY-MM-DD format
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # If already in correct format, return as-is
    if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str
    
    print(f"⚠️  Could not parse date: {date_str}")
    return None

def parse_json_file(file_path: str) -> Dict:
    """Parse JSON file and return head/data structure"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if isinstance(data, dict) and 'head' in data and 'data' in data:
        return {
            'headers': data['head'],
            'rows': data['data']
        }
    return None


def process_bonus_data(file_path: str, market_code: str, db: Session) -> int:
    """
    Process bonus issue data from NSE/BSE
    
    Expected columns:
    [Company Name, ISIN, Security Type, Announcement Date, Ex-Date, Numerator, Denominator]
    """
    print(f"📊 Processing {market_code} bonus data: {file_path}")
    
    parsed = parse_json_file(file_path)
    if not parsed:
        print(f"❌ Could not parse {file_path}")
        return 0
    
    rows = parsed['rows']
    added_count = 0
    
    # Get unique company names for batch fetching security IDs
    unique_companies = list(set([row[0] for row in rows if row and len(row) > 0]))
    print(f"🔍 Fetching security IDs for {len(unique_companies)} companies...")
    security_map = alfago_client.fetch_security_batch(unique_companies, market_code, delay=0.05)
    
    for row in rows:
        if not row or len(row) < 7:
            continue
        
        try:
            company_name = row[0]
            isin = row[1] if row[1] != 'N.A.' else None
            security_type = row[2]
            announcement_date = parse_date(row[3]) if row[3] else None
            ex_date = parse_date(row[4]) if row[4] else None
            ratio_num = float(row[5]) if row[5] and row[5] != 'N.A.' else None
            ratio_den = float(row[6]) if row[6] and row[6] != 'N.A.' else None
            
            # Get security ID
            security_info = security_map.get(company_name)
            security_id = security_info['security_id'] if security_info else None
            symbol = security_info['symbol'] if security_info else None
            
            # Check if record already exists
            existing = db.query(CorporateAction).filter(
                CorporateAction.company_name == company_name,
                CorporateAction.action_type == 'bonus',
                CorporateAction.market_code == market_code,
                CorporateAction.ex_date == ex_date
            ).first()
            
            if existing:
                continue
            
            # Create new record
            action = CorporateAction(
                company_name=company_name,
                security_id=security_id,
                market_code=market_code,
                symbol=symbol,
                isin=isin,
                action_type='bonus',
                announcement_date=announcement_date,
                ex_date=ex_date,
                final_date=ex_date,
                ratio_numerator=ratio_num,
                ratio_denominator=ratio_den,
                security_type=security_type,
                raw_data=json.dumps(row)
            )
            
            db.add(action)
            added_count += 1
            
        except Exception as e:
            print(f"⚠️  Error processing bonus row: {e}")
            continue
    
    db.commit()
    print(f"✅ Added {added_count} bonus records for {market_code}")
    return added_count


def process_dividend_data(file_path: str, market_code: str, db: Session) -> int:
    """
    Process dividend data from NSE/BSE
    
    Expected columns:
    [Company Name, Announcement Date, Ex-Date, Dividend Rate, Dividend Type, Record Date]
    """
    print(f"💰 Processing {market_code} dividend data: {file_path}")
    
    parsed = parse_json_file(file_path)
    if not parsed:
        print(f"❌ Could not parse {file_path}")
        return 0
    
    rows = parsed['rows']
    added_count = 0
    
    # Get unique company names for batch fetching security IDs
    unique_companies = list(set([row[0] for row in rows if row and len(row) > 0]))
    print(f"🔍 Fetching security IDs for {len(unique_companies)} companies...")
    security_map = alfago_client.fetch_security_batch(unique_companies, market_code, delay=0.05)
    
    for row in rows:
        if not row or len(row) < 6:
            continue
        
        try:
            company_name = row[0]
            announcement_date = parse_date(row[1]) if row[1] else None
            ex_date = parse_date(row[2]) if row[2] else None
            dividend_rate = float(row[3]) if row[3] and row[3] != 'N.A.' else None
            dividend_type = row[4]
            record_date = parse_date(row[5]) if row[5] else None
            
            # Get security ID
            security_info = security_map.get(company_name)
            security_id = security_info['security_id'] if security_info else None
            symbol = security_info['symbol'] if security_info else None
            isin = security_info['isin'] if security_info else None
            
            # Check if record already exists
            existing = db.query(CorporateAction).filter(
                CorporateAction.company_name == company_name,
                CorporateAction.action_type == 'dividend',
                CorporateAction.market_code == market_code,
                CorporateAction.ex_date == ex_date
            ).first()
            
            if existing:
                continue
            
            # Create new record
            action = CorporateAction(
                company_name=company_name,
                security_id=security_id,
                market_code=market_code,
                symbol=symbol,
                isin=isin,
                action_type='dividend',
                announcement_date=announcement_date,
                ex_date=ex_date,
                record_date=record_date,
                final_date=ex_date,
                dividend_rate=dividend_rate,
                dividend_type=dividend_type,
                raw_data=json.dumps(row)
            )
            
            db.add(action)
            added_count += 1
            
        except Exception as e:
            print(f"⚠️  Error processing dividend row: {e}")
            continue
    
    db.commit()
    print(f"✅ Added {added_count} dividend records for {market_code}")
    return added_count


def process_split_data(file_path: str, market_code: str, db: Session) -> int:
    """
    Process stock split data from NSE/BSE
    
    Expected columns:
    [Company Name, Capital issue Type, Security Type, Date of Announcement, Capital issue Date,
     Ratio Numerator, Ratio Denominator, X Price, X Date, Returns on X Date]
    """
    print(f"✂️  Processing {market_code} split data: {file_path}")
    
    parsed = parse_json_file(file_path)
    if not parsed:
        print(f"❌ Could not parse {file_path}")
        return 0
    
    rows = parsed['rows']
    added_count = 0
    
    # Get unique company names
    unique_companies = list(set([row[0] for row in rows if row and len(row) > 0]))
    print(f"🔍 Fetching security IDs for {len(unique_companies)} companies...")
    security_map = alfago_client.fetch_security_batch(unique_companies, market_code, delay=0.05)
    
    for row in rows:
        if not row or len(row) < 7:
            continue
        
        try:
            company_name = row[0]
            # capital_type = row[1]  # Should be "Split"
            # security_type = row[2]  # "Equity shares"
            announcement_date = parse_date(row[3])  # Returns YYYY-MM-DD string
            ex_date = parse_date(row[4])  # Returns YYYY-MM-DD string
            ratio_num = float(row[5]) if row[5] and row[5] != 'N.A.' else None
            ratio_den = float(row[6]) if row[6] and row[6] != 'N.A.' else None
            split_ratio = f"{ratio_num}:{ratio_den}" if ratio_num and ratio_den else None
            
            # Get security ID from Alfago
            security_info = security_map.get(company_name)
            security_id = security_info['security_id'] if security_info else None
            symbol = security_info['symbol'] if security_info else None
            isin = security_info['isin'] if security_info else None
            
            # Check if record already exists
            existing = db.query(CorporateAction).filter(
                CorporateAction.company_name == company_name,
                CorporateAction.action_type == 'split',
                CorporateAction.market_code == market_code,
                CorporateAction.ex_date == ex_date
            ).first()
            
            if existing:
                continue
            
            # Create new record
            action = CorporateAction(
                company_name=company_name,
                security_id=security_id,
                market_code=market_code,
                symbol=symbol,
                isin=isin,
                action_type='split',
                announcement_date=announcement_date,
                ex_date=ex_date,
                final_date=ex_date,
                ratio_numerator=ratio_num,
                ratio_denominator=ratio_den,
                split_ratio=split_ratio,
                raw_data=json.dumps(row)
            )
            
            db.add(action)
            added_count += 1
            
        except Exception as e:
            print(f"⚠️  Error processing split row: {e}")
            continue
    
    db.commit()
    print(f"✅ Added {added_count} split records for {market_code}")
    return added_count


def process_rights_data(file_path: str, market_code: str, db: Session) -> int:
    """
    Process rights issue data from NSE
    
    Expected columns:
    [Company Name, ISIN, Announcement Date, Ex-Date, Ratio Num, Ratio Den, Rights Price]
    """
    print(f"📝 Processing {market_code} rights data: {file_path}")
    
    parsed = parse_json_file(file_path)
    if not parsed:
        print(f"❌ Could not parse {file_path}")
        return 0
    
    rows = parsed['rows']
    added_count = 0
    
    # Get unique company names
    unique_companies = list(set([row[0] for row in rows if row and len(row) > 0]))
    print(f"🔍 Fetching security IDs for {len(unique_companies)} companies...")
    security_map = alfago_client.fetch_security_batch(unique_companies, market_code, delay=0.05)
    
    for row in rows:
        if not row or len(row) < 9:
            continue
        
        try:
            company_name = row[0]
            issue_type = row[1]  # Should be "Rights"
            announcement_date = parse_date(row[3]) if row[3] and row[3] != 'N.A.' else None  # Returns YYYY-MM-DD
            ex_date = parse_date(row[4]) if row[4] and row[4] != 'N.A.' else None  # Returns YYYY-MM-DD
            
            rights_num = float(row[7]) if row[7] and row[7] != 'N.A.' else None
            rights_den = float(row[8]) if row[8] and row[8] != 'N.A.' else None
            rights_price = float(row[6]) if row[6] and row[6] != 'N.A.' else None
            isin = None  # Not in this dataset
            
            # Get security ID
            security_info = security_map.get(company_name)
            security_id = security_info['security_id'] if security_info else None
            symbol = security_info['symbol'] if security_info else None
            
            # Check if record already exists
            existing = db.query(CorporateAction).filter(
                CorporateAction.company_name == company_name,
                CorporateAction.action_type == 'rights',
                CorporateAction.market_code == market_code,
                CorporateAction.ex_date == ex_date
            ).first()
            
            if existing:
                continue
            
            # Create new record
            action = CorporateAction(
                company_name=company_name,
                security_id=security_id,
                market_code=market_code,
                symbol=symbol,
                isin=isin,
                action_type='rights',
                announcement_date=announcement_date,
                ex_date=ex_date,
                final_date=ex_date,
                rights_ratio_numerator=rights_num,
                rights_ratio_denominator=rights_den,
                rights_price=rights_price,
                raw_data=json.dumps(row)
            )
            
            db.add(action)
            added_count += 1
            
        except Exception as e:
            print(f"⚠️  Error processing rights row: {e}")
            continue
    
    db.commit()
    print(f"✅ Added {added_count} rights records for {market_code}")
    return added_count


def process_all_files(tmp_dir: str = "./tmp") -> Dict[str, int]:
    """
    Process all corporate action files from tmp directory
    
    Returns:
        Dict with counts for each action type and market
    """
    if not os.path.exists(tmp_dir):
        print(f"❌ Directory not found: {tmp_dir}")
        return {}
    
    db = SessionLocal()
    stats = {}
    
    try:
        # File mappings (filename pattern -> (action_handler, market_code))
        file_handlers = {
            'bonus_nse': (process_bonus_data, 'NSE'),
            'bonus_bse': (process_bonus_data, 'BSE'),
            'dividend_nse': (process_dividend_data, 'NSE'),
            'dividend_bse': (process_dividend_data, 'BSE'),
            'splits_nse': (process_split_data, 'NSE'),
            'splits_bse': (process_split_data, 'BSE'),
            'rights_nse': (process_rights_data, 'NSE'),
        }
        
        # Process each file type
        for file_pattern, (handler, market) in file_handlers.items():
            # Look for JSON files matching the pattern
            json_files = [f for f in os.listdir(tmp_dir) 
                         if f.endswith('.json') and file_pattern.lower() in f.lower()]
            
            for json_file in json_files:
                file_path = os.path.join(tmp_dir, json_file)
                count = handler(file_path, market, db)
                key = f"{file_pattern}_{market}"
                stats[key] = stats.get(key, 0) + count
        
        return stats
        
    finally:
        db.close()


if __name__ == "__main__":
    print("🚀 Corporate Actions Comprehensive Processor")
    print("=" * 60)
    
    stats = process_all_files()
    
    print("\n" + "=" * 60)
    print("📊 PROCESSING SUMMARY")
    print("=" * 60)
    
    if stats:
        for key, count in stats.items():
            print(f"✅ {key}: {count} records")
    else:
        print("⚠️  No files processed")
