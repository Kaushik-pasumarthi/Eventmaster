import requests, zipfile, io, os, json, pandas as pd, time
import config

def send_batch(batch_path, fmt="json"):
    files = {"batchfile": open(batch_path, "rb")}
    data = {"apikey": config.PROWESS_API_KEY, "format": fmt}
    r = requests.post(config.SENDBATCH_URL, data=data, files=files)
    files["batchfile"].close()
    r.raise_for_status()
    j = r.json()
    if "token" not in j:
        raise Exception(f"Bad response: {j}")
    return j["token"]

def get_batch(token, poll_interval=3, timeout=300):
    start = time.time()
    elapsed = 0
    while True:
        r = requests.post(config.GETBATCH_URL, data={"apikey": config.PROWESS_API_KEY, "token": token})
        if "application/json" not in r.headers.get("Content-Type", "").lower() and r.content[:2] == b"PK":
            z = zipfile.ZipFile(io.BytesIO(r.content))
            files = []
            for name in z.namelist():
                if name.lower().endswith(".lst"): continue
                out_path = os.path.join(config.TMP_DIR, os.path.basename(name))
                with z.open(name) as src, open(out_path, "wb") as out:
                    out.write(src.read())
                files.append(out_path)
            return files
        elif time.time() - start > timeout:
            raise TimeoutError("Timeout waiting for batch")
        else:
            elapsed = int(time.time() - start)
            print(f"Still processing batch... ({elapsed}s elapsed)"); time.sleep(poll_interval)

def parse_json_files(paths):
    """Parse JSON files from CMIE API response with support for complex headers"""
    dfs = []
    parsed_data = {"files": [], "total_rows": 0}
    
    for p in paths:
        if p.endswith(".json"):
            with open(p) as f: 
                js = json.load(f)
            
            # Handle different JSON structures from CMIE
            if isinstance(js, dict) and "data" in js and "head" in js:
                # CMIE format with head and data
                headers = js["head"]
                data = js["data"]
                
                # Handle multi-level headers
                if isinstance(headers[0], list) and len(headers[0]) > 1:
                    # Multi-level header - find the actual column names
                    # Look for the row with actual column names (usually the last non-empty row)
                    actual_headers = None
                    for header_row in headers:
                        if header_row and any(h and h.strip() for h in header_row):
                            # Check if this looks like actual column names
                            if any(h for h in header_row if h and not h.startswith("Output source") and h != ""):
                                actual_headers = header_row
                    
                    if actual_headers is None:
                        # Fallback - use the last header row
                        actual_headers = headers[-1] if headers else []
                    
                    # Clean up headers - remove empty strings and None values
                    clean_headers = []
                    for i, h in enumerate(actual_headers):
                        if h and h.strip():
                            clean_headers.append(h.strip())
                        else:
                            clean_headers.append(f"Column_{i+1}")
                    
                    # Ensure we have the right number of columns
                    num_cols = len(data[0]) if data else len(clean_headers)
                    while len(clean_headers) < num_cols:
                        clean_headers.append(f"Column_{len(clean_headers)+1}")
                    
                    # Create DataFrame
                    df = pd.DataFrame(data, columns=clean_headers[:num_cols])
                    
                else:
                    # Simple header
                    df = pd.DataFrame(data, columns=headers[0] if isinstance(headers[0], list) else headers)
                
                parsed_data["files"].append({
                    "filename": os.path.basename(p),
                    "structure": "head_data_cmie",
                    "columns": list(df.columns),
                    "rows": len(data)
                })
                
            elif isinstance(js, list):
                # Array of objects
                df = pd.DataFrame(js)
                parsed_data["files"].append({
                    "filename": os.path.basename(p),
                    "structure": "array",
                    "columns": list(df.columns) if not df.empty else [],
                    "rows": len(js)
                })
            elif isinstance(js, dict):
                # Direct object
                df = pd.DataFrame([js])
                parsed_data["files"].append({
                    "filename": os.path.basename(p),
                    "structure": "object",
                    "columns": list(df.columns) if not df.empty else [],
                    "rows": 1
                })
            else:
                continue
                
            if not df.empty:
                # Add metadata
                df['source_file'] = os.path.basename(p)
                df['parsed_at'] = pd.Timestamp.now()
                dfs.append(df)
    
    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    parsed_data["total_rows"] = len(final_df)
    
    return final_df, parsed_data

def get_data_type_from_filename(filename):
    """Infer data type from filename"""
    filename_lower = filename.lower()
    if 'equity' in filename_lower or 'ownership' in filename_lower:
        return 'equity_ownership'
    elif 'financial' in filename_lower or 'results' in filename_lower:
        return 'financial_results'
    elif 'balance' in filename_lower:
        return 'balance_sheet'
    elif 'ratio' in filename_lower:
        return 'financial_ratios'
    elif 'company' in filename_lower:
        return 'company_info'
    else:
        return 'general_data'
