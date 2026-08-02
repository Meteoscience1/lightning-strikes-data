import os
import json
import datetime
import urllib.parse
import requests
import s3fs
import xarray as xr
import pandas as pd
import boto3
from botocore.client import Config

# --- CONFIGURATION & ENV VARIABLES ---
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "meteoscience-lightning")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL")  # e.g., "https://pub-xxxx.r2.dev"

EUMETSAT_CONSUMER_KEY = os.getenv("EUMETSAT_CONSUMER_KEY")
EUMETSAT_CONSUMER_SECRET = os.getenv("EUMETSAT_CONSUMER_SECRET")

# File name for the output
OUTPUT_FILE = "recent_strikes.json"
STRIKE_WINDOW_MINUTES = 30  # Keep strikes from the last 30 minutes

# Setup R2 client if keys are available
s3_client = None
if R2_ACCOUNT_ID and R2_ACCESS_KEY and R2_SECRET_KEY:
    r2_endpoint = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    s3_client = boto3.client(
        "s3",
        endpoint_url=r2_endpoint,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

# --- UTILITIES ---
def get_utc_now():
    return datetime.datetime.now(datetime.timezone.utc)

def download_existing_strikes():
    """Downloads existing strikes from R2 or local file to perform incremental updates."""
    if s3_client:
        try:
            print("Downloading existing strikes from R2...")
            response = s3_client.get_object(Bucket=R2_BUCKET_NAME, Key=OUTPUT_FILE)
            return json.loads(response["Body"].read().decode("utf-8"))
        except Exception as e:
            print(f"No existing strikes found in R2 (or error: {e}). Starting fresh.")
    elif os.path.exists(OUTPUT_FILE):
        try:
            print("Reading existing strikes from local file...")
            with open(OUTPUT_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error reading local file: {e}. Starting fresh.")
    return []

def upload_strikes(strikes):
    """Uploads strikes list to R2 or writes to a local file."""
    payload = {
        "updated_at": int(get_utc_now().timestamp()),
        "count": len(strikes),
        "strikes": strikes
    }
    json_data = json.dumps(payload, separators=(',', ':')) # Minified JSON
    
    if s3_client:
        try:
            print("Uploading strikes to R2...")
            s3_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=OUTPUT_FILE,
                Body=json_data.encode("utf-8"),
                ContentType="application/json",
                CacheControl="max-age=30"  # Cache on CDN for 30 seconds
            )
            print("Upload completed successfully!")
            if R2_PUBLIC_URL:
                print(f"Public URL: {R2_PUBLIC_URL}/{OUTPUT_FILE}")
            return
        except Exception as e:
            print(f"Failed to upload to R2: {e}")
            
    # Fallback to local file
    print(f"Writing strikes to local file: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, "w") as f:
        f.write(json_data)

# --- NOAA GOES GLM S3 PARSER ---
def parse_goes_glm_bucket(bucket_name, lookback_minutes=5):
    """Parses real-time GOES GLM files from public NOAA S3 buckets."""
    print(f"Fetching data from NOAA bucket: {bucket_name}...")
    fs = s3fs.S3FileSystem(anon=True)
    now = get_utc_now()
    
    # We will list files for the current hour, and the previous hour (just in case)
    hours_to_check = [now, now - datetime.timedelta(hours=1)]
    file_paths = []
    
    for dt in hours_to_check:
        year = dt.year
        doy = dt.timetuple().tm_yday
        hour = dt.hour
        prefix = f"{bucket_name}/GLM-L2-LCFA/{year}/{doy:03d}/{hour:02d}/"
        try:
            files = fs.list_folders(prefix) if hasattr(fs, "list_folders") else fs.ls(prefix)
            file_paths.extend(files)
        except Exception as e:
            # Bucket directory might not exist yet if it is the very first minute of the hour/day
            continue
            
    if not file_paths:
        print(f"No files found in NOAA {bucket_name} for the checked hours.")
        return []
        
    # Sort files chronologically (they contain timestamp in the filename)
    file_paths.sort()
    
    # Filename format: OR_GLM-L2-LCFA_G16_sYYYYJJJHHMMSS...
    # We filter files whose start time is within our lookback window
    cutoff_time = now - datetime.timedelta(minutes=lookback_minutes)
    target_files = []
    
    for path in file_paths:
        filename = path.split("/")[-1]
        if not filename.startswith("OR_GLM-L2-LCFA"):
            continue
        try:
            # Parse start time from filename (e.g. s20262041830000 -> YYYY DOY HHMMSS)
            # The start time starts at index 21 of the filename
            start_str = filename.split("_")[3][1:14] # e.g. "2026204183000"
            file_dt = datetime.datetime.strptime(start_str, "%Y%j%H%M%S")
            file_dt = file_dt.replace(tzinfo=datetime.timezone.utc)
            if file_dt >= cutoff_time:
                target_files.append(path)
        except Exception as e:
            # If filename structure is unexpected, skip
            continue
            
    print(f"Found {len(target_files)} files in lookback window ({lookback_minutes} mins).")
    
    new_strikes = []
    for path in target_files:
        try:
            # Open NetCDF directly from S3
            with fs.open(path) as f:
                ds = xr.open_dataset(f, engine="h5netcdf")
                
                # GLM variable names: flash_lat, flash_lon, flash_time_threshold
                if "flash_lat" in ds and "flash_lon" in ds:
                    if ds["flash_lat"].size == 0:
                        ds.close()
                        continue
                        
                    # Get baseline timestamp from file attributes (e.g. time_coverage_start)
                    ts = int(get_utc_now().timestamp())
                    time_str = ds.attrs.get("time_coverage_start")
                    if time_str:
                        try:
                            ts = int(pd.Timestamp(time_str).timestamp())
                        except Exception as parse_err:
                            print(f"Failed to parse time_coverage_start '{time_str}': {parse_err}")
                    
                    if ds["flash_lat"].ndim == 0:
                        lats = [float(ds["flash_lat"].values)]
                        lons = [float(ds["flash_lon"].values)]
                    else:
                        lats = ds["flash_lat"].values
                        lons = ds["flash_lon"].values
                        
                    for lat, lon in zip(lats, lons):
                        new_strikes.append({
                            "lat": round(float(lat), 3),
                            "lon": round(float(lon), 3),
                            "time": ts
                        })
                ds.close()
        except Exception as e:
            # Skip corrupted files or read errors
            print(f"Error parsing file {path}: {e}")
            continue
            
    return new_strikes

# --- EUMETSAT MTG LIGHTNING IMAGER (LI) API ---
def get_eumetsat_token():
    """Obtains oauth token from EUMETSAT Data Store API."""
    if not EUMETSAT_CONSUMER_KEY or not EUMETSAT_CONSUMER_SECRET:
        return None
    try:
        url = "https://api.eumetsat.int/token"
        response = requests.post(
            url,
            auth=(EUMETSAT_CONSUMER_KEY, EUMETSAT_CONSUMER_SECRET),
            data={"grant_type": "client_credentials"},
            timeout=10
        )
        if response.status_code == 200:
            return response.json().get("access_token")
        else:
            print(f"EUMETSAT token request failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error authenticating with EUMETSAT: {e}")
    return None

def parse_eumetsat_mtg_li(lookback_minutes=5):
    """Fetches and parses Meteosat Third Generation (MTG) Lightning Imager (LI) flashes."""
    token = get_eumetsat_token()
    if not token:
        print("Skipping EUMETSAT MTG LI (No valid credentials/auth).")
        return []
        
    print("Fetching EUMETSAT MTG LI data...")
    headers = {"Authorization": f"Bearer {token}"}
    
    # Collection: EO:EUM:DAT:0691 (MTG LI Lightning Flashes)
    collection_id = "EO:EUM:DAT:0691"
    now = get_utc_now()
    start_time = (now - datetime.timedelta(minutes=lookback_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Search for products in the time window
    search_url = (
        f"https://api.eumetsat.int/data/search-sal/os/1.0/search"
        f"?q=CollectionId:{collection_id} AND dtstart:[{start_time} TO {end_time}]"
        f"&format=json"
    )
    
    try:
        res = requests.get(search_url, headers=headers, timeout=15)
        if res.status_code != 200:
            print(f"EUMETSAT search query failed: {res.status_code}")
            return []
            
        data = res.json()
        entries = data.get("feed", {}).get("entry", [])
        if not entries:
            print("No new EUMETSAT MTG LI products found in time window.")
            return []
            
        new_strikes = []
        # Download and parse each product (typically files are created every few minutes)
        for entry in entries:
            product_id = entry.get("id", {}).get("value")
            if not product_id:
                continue
                
            # Download URL
            download_url = f"https://api.eumetsat.int/data/download/1.0.0/collections/{collection_id}/products/{product_id}"
            prod_res = requests.get(download_url, headers=headers, stream=True, timeout=30)
            
            if prod_res.status_code != 200:
                print(f"Failed to download product {product_id}: {prod_res.status_code}")
                continue
                
            # Read downloaded content in-memory as NetCDF
            content = prod_res.content
            # To open netCDF from bytes in memory, we can write it to a temp file
            temp_file_path = f"/tmp/{product_id}.nc"
            os.makedirs("/tmp", exist_ok=True)
            with open(temp_file_path, "wb") as temp_file:
                temp_file.write(content)
                
            try:
                ds = xr.open_dataset(temp_file_path, engine="h5netcdf")
                # EUMETSAT MTG variables: typically flash_latitude, flash_longitude, flash_time
                # Let's inspect variables dynamically
                lat_var = next((v for v in ds.variables if "lat" in v.lower()), None)
                lon_var = next((v for v in ds.variables if "lon" in v.lower()), None)
                time_var = next((v for v in ds.variables if "time" in v.lower()), None)
                
                if lat_var and lon_var:
                    lats = ds[lat_var].values
                    lons = ds[lon_var].values
                    # If time variable exists, parse it, otherwise fallback to product time
                    times = ds[time_var].values if time_var else [now.timestamp()] * len(lats)
                    
                    for lat, lon, time in zip(lats, lons, times):
                        # Convert time to timestamp
                        ts = int(time) if not isinstance(time, str) else int(pd.Timestamp(time).timestamp())
                        new_strikes.append({
                            "lat": round(float(lat), 3),
                            "lon": round(float(lon), 3),
                            "time": ts
                        })
                ds.close()
            except Exception as e:
                print(f"Error parsing EUMETSAT MTG LI product {product_id}: {e}")
            finally:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
                    
        return new_strikes
        
    except Exception as e:
        print(f"Error running EUMETSAT search/download: {e}")
        
    return []

# --- MAIN EXECUTION PIPELINE ---
def main():
    print(f"=== Lightning Pipeline Start: {get_utc_now().isoformat()} ===")
    
    # 1. Download existing strikes
    existing_strikes = download_existing_strikes()
    
    # 2. Parse NOAA GOES-16 (East) and GOES-18 (West)
    new_strikes = []
    
    try:
        new_strikes.extend(parse_goes_glm_bucket("noaa-goes16", lookback_minutes=3))
    except Exception as e:
        print(f"Error fetching GOES-16: {e}")
        
    try:
        new_strikes.extend(parse_goes_glm_bucket("noaa-goes18", lookback_minutes=3))
    except Exception as e:
        print(f"Error fetching GOES-18: {e}")
        
    # 3. Parse EUMETSAT MTG-LI
    try:
        new_strikes.extend(parse_eumetsat_mtg_li(lookback_minutes=3))
    except Exception as e:
        print(f"Error fetching EUMETSAT MTG: {e}")
        
    print(f"Retrieved {len(new_strikes)} new strikes across all sources.")
    
    # 4. Merge new strikes with existing strikes
    existing_list = []
    if isinstance(existing_strikes, dict):
        existing_list = existing_strikes.get("strikes", [])
    elif isinstance(existing_strikes, list):
        existing_list = existing_strikes
        
    merged_strikes = existing_list + new_strikes
    
    # 5. Deduplicate and filter old strikes
    cutoff_ts = int((get_utc_now() - datetime.timedelta(minutes=STRIKE_WINDOW_MINUTES)).timestamp())
    
    # Deduplicate using a unique key: (lat_rounded, lon_rounded, time)
    # Using 3 decimal places for lat/lon, and grouping times to nearest 5 seconds to handle minor variations
    unique_strikes = {}
    for strike in merged_strikes:
        ts = strike["time"]
        if ts < cutoff_ts:
            continue  # Discard old strikes
            
        lat_rounded = round(strike["lat"], 3)
        lon_rounded = round(strike["lon"], 3)
        ts_bucket = (ts // 5) * 5  # Group to 5 second intervals to prevent duplication
        
        key = (lat_rounded, lon_rounded, ts_bucket)
        # Keep the latest matching strike representation
        unique_strikes[key] = {
            "lat": lat_rounded,
            "lon": lon_rounded,
            "time": ts
        }
        
    cleaned_strikes = list(unique_strikes.values())
    # Sort chronologically, latest strikes last
    cleaned_strikes.sort(key=lambda x: x["time"])
    
    print(f"Merged and cleaned: {len(cleaned_strikes)} active strikes in the last {STRIKE_WINDOW_MINUTES} mins.")
    
    # 6. Save/Upload results
    upload_strikes(cleaned_strikes)
    print("=== Lightning Pipeline Completed ===")

if __name__ == "__main__":
    main()
