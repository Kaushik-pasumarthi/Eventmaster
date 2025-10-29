#!/usr/bin/env python3
"""
Daily Corporate Actions Updater - Enhanced Version
- Fetches data from ALL corporate action types (bonus, dividend, splits, rights) for NSE and BSE
- Adds security IDs from Alfago API
- Processes and stores in PostgreSQL database
"""
import prowess_client
import corporate_actions_processor
from datetime import datetime, timedelta
import os
import shutil
from database import SessionLocal
from models import CorporateAction

def fetch_fresh_data():
    """Fetch fresh data from all batch files"""
    print(" Fetching fresh data from Prowess...")
    print("=" * 60)
    
    # Map batch files to expected output names
    batch_files = [
        ("bonus_nse.bt", " NSE Bonus Issues", "bonus_nse.json"),
        ("bonus_bse.bt", " BSE Bonus Issues", "bonus_bse.json"),
        ("dividend_nse.bt", " NSE Dividends", "dividend_nse.json"),
        ("dividend_bse.bt", " BSE Dividends", "dividend_bse.json"),
        ("splits_nse.bt", " NSE Stock Splits", "splits_nse.json"),
        ("splits_bse.bt", " BSE Stock Splits", "splits_bse.json"),
        ("rights_nse.bt", " NSE Rights Issues", "rights_nse.json"),
    ]
    
    successful_fetches = 0
    total_files = len(batch_files)
    
    for idx, (batch_file, description, target_name) in enumerate(batch_files, 1):
        print(f"\n[{idx}/{total_files}] {description}")
        print(f"Fetching {batch_file}...")
        
        try:
            token = prowess_client.send_batch(batch_file, "json")
            files = prowess_client.get_batch(token)
            
            # Find and rename the JSON file with actual data (not summary)
            if files and len(files) > 0:
                target_path = os.path.join("./tmp", target_name)
                renamed = False
                
                # Try to find the file with most data (not just company list)
                for source_file in files:
                    if os.path.exists(source_file) and source_file.endswith('.json'):
                        # Check if this file has the detailed data structure
                        try:
                            import json
                            with open(source_file, 'r') as f:
                                data = json.load(f)
                                # Look for files with more than 2 columns (detailed data)
                                if 'data' in data and len(data['data']) > 0 and len(data['data'][0]) > 2:
                                    shutil.copy(source_file, target_path)  # Use copy instead of move
                                    print(f" Got {len(files)} file(s), saved as {target_name}")
                                    successful_fetches += 1
                                    renamed = True
                                    break
                        except:
                            continue
                
                if not renamed:
                    print(f" No detailed data file found in {len(files)} file(s)")
            else:
                print(f" No files received")
                
        except FileNotFoundError:
            print(f" File not found: {batch_file}")
        except Exception as e:
            print(f" Fetch error: {e}")
    
    print("\n" + "=" * 60)
    print(f" Fetch Summary: {successful_fetches}/{len(batch_files)} successful")
    print("=" * 60)
    
    return successful_fetches


def update_database():
    """Update database with freshly fetched data"""
    print("\n Processing and updating database...")
    print("=" * 60)
    
    # Process all files and update database
    stats = corporate_actions_processor.process_all_files()
    
    if not stats:
        print("  No data was processed")
        return
    
    print("\n Database Update Summary:")
    print("=" * 60)
    
    total_records = 0
    for key, count in stats.items():
        print(f"{key}: {count} new records")
        total_records += count
    
    print("=" * 60)
    print(f" Total: {total_records} new records added")
    print("=" * 60)


def cleanup_old_records(days_old=10):
    """Remove records whose final_date is older than specified days"""
    print(f"\n Cleaning up records older than {days_old} days...")
    print("=" * 60)
    
    db = SessionLocal()
    try:
        # Calculate cutoff date
        cutoff_date = (datetime.now() - timedelta(days=days_old)).strftime('%Y-%m-%d')
        
        # Find old records
        old_records = db.query(CorporateAction).filter(
            CorporateAction.final_date < cutoff_date
        ).all()
        
        count = len(old_records)
        
        if count == 0:
            print(f" No records older than {days_old} days found")
            return 0
        
        # Delete old records
        for record in old_records:
            db.delete(record)
        
        db.commit()
        print(f" Deleted {count} old records (final_date < {cutoff_date})")
        return count
        
    except Exception as e:
        print(f" Error during cleanup: {e}")
        db.rollback()
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    # Set UTF-8 encoding for console output
    import sys
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
    
    # Force unbuffered output
    import functools
    print = functools.partial(print, flush=True)
    
    print("\n" + "=" * 70)
    print("  CORPORATE ACTIONS DAILY UPDATER")
    print("  Enhanced with Security IDs & Multi-Market Support")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Step 1: Fetch fresh data
    fetch_count = fetch_fresh_data()
    
    if fetch_count == 0:
        print("\nNo data fetched. Exiting.")
        exit(1)
    
    # Step 2: Update database
    update_database()
    
    # Step 3: Cleanup old records (10 days old)
    cleanup_old_records(days_old=10)
    
    print("\n" + "=" * 70)
    print(f" Update completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
