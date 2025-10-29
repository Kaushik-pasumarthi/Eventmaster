"""
Alfago Security ID Fetcher
Fetches security_id, symbol, ISIN from Alfago API based on company name
"""
import requests
import time
from typing import Dict, Optional, List

# Cache to avoid repeated API calls
_SECURITY_CACHE = {}

def fetch_security_id(company_name: str, market_code: str = None) -> Optional[Dict]:
    """
    Fetch security ID and details from Alfago API
    
    Args:
        company_name: Company name to search
        market_code: 'NSE' or 'BSE' preference (optional)
    
    Returns:
        Dict with security_id, symbol, isin, market_code or None if not found
    """
    # Check cache first
    cache_key = f"{company_name}_{market_code or 'ANY'}"
    if cache_key in _SECURITY_CACHE:
        return _SECURITY_CACHE[cache_key]
    
    try:
        # Call Alfago API
        url = f"https://alfago.in/api/alfagrow/security/get/{company_name}"
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            print(f"⚠️  API error for {company_name}: Status {response.status_code}")
            return None
        
        if 'application/json' not in response.headers.get('content-type', ''):
            print(f"⚠️  Non-JSON response for {company_name}")
            return None
        
        data = response.json()
        
        if data.get('status') != 'success' or not data.get('data'):
            print(f"⚠️  No data found for {company_name}")
            return None
        
        securities = data['data']
        
        # If market_code is specified, try to find matching market
        if market_code:
            for sec in securities:
                # Check if market_code1 or market_code2 matches
                if sec.get('market_code1') == market_code or sec.get('market_code2') == market_code:
                    result = {
                        'security_id': sec.get('id'),
                        'symbol': sec.get('symbol1') or sec.get('symbol2'),
                        'isin': sec.get('isin'),
                        'market_code': market_code,
                        'company_name': sec.get('company_name')
                    }
                    _SECURITY_CACHE[cache_key] = result
                    return result
        
        # Default: return first result
        sec = securities[0]
        # Determine primary market (prefer NSE over BSE)
        primary_market = sec.get('market_code1') or sec.get('market_code2') or market_code
        
        result = {
            'security_id': sec.get('id'),
            'symbol': sec.get('symbol1') or sec.get('symbol2'),
            'isin': sec.get('isin'),
            'market_code': primary_market,
            'company_name': sec.get('company_name')
        }
        
        _SECURITY_CACHE[cache_key] = result
        return result
        
    except requests.exceptions.Timeout:
        print(f"⏱️  Timeout fetching {company_name}")
        return None
    except Exception as e:
        print(f"❌ Error fetching {company_name}: {str(e)}")
        return None


def fetch_security_batch(company_names: List[str], market_code: str = None, delay: float = 0.05) -> Dict[str, Dict]:
    """
    Fetch security IDs for multiple companies with rate limiting
    
    Args:
        company_names: List of company names
        market_code: 'NSE' or 'BSE' preference
        delay: Delay between requests in seconds (default 0.05)
    
    Returns:
        Dict mapping company_name -> security info
    """
    results = {}
    total = len(company_names)
    found = 0
    not_found = 0
    
    print(f"📊 Fetching security IDs for {total} companies (this may take ~{int(total * delay)} seconds)...")
    
    for idx, company_name in enumerate(company_names, 1):
        # Progress indicator every 10 companies
        if idx % 10 == 0 or idx == total:
            print(f"   Progress: {idx}/{total} ({int(idx/total*100)}%) - Found: {found}, Not found: {not_found}")
        
        security_info = fetch_security_id(company_name, market_code)
        if security_info:
            results[company_name] = security_info
            found += 1
        else:
            not_found += 1
        
        # Rate limiting
        if idx < total:
            time.sleep(delay)
    
    print(f"✅ Complete: {found} found, {not_found} not found out of {total} companies")
    return results


def clear_cache():
    """Clear the security ID cache"""
    global _SECURITY_CACHE
    _SECURITY_CACHE = {}
    print("🗑️  Security cache cleared")


if __name__ == "__main__":
    # Test the module
    print("Testing Alfago Security ID Fetcher\n")
    
    test_companies = [
        ("Paushak Ltd.", "NSE"),
        ("HDFC Bank Ltd.", "BSE"),
        ("Reliance Industries Ltd.", "NSE")
    ]
    
    for company, market in test_companies:
        print(f"\n🔍 Testing: {company} ({market})")
        result = fetch_security_id(company, market)
        if result:
            print(f"✅ Result: {result}")
        else:
            print(f"❌ No result")
