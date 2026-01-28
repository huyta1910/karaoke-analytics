import os
import requests
from dotenv import load_dotenv

# --- SETUP ---
load_dotenv()

PAGE_ID = os.getenv('FB_PAGE_ID')
ACCESS_TOKEN = os.getenv('FB_ACCESS_TOKEN')

if not PAGE_ID or not ACCESS_TOKEN:
    print("❌ Error: Could not find FB_PAGE_ID or FB_ACCESS_TOKEN in your .env file.")
    exit()

print(f"--- Debugging Metrics for Page ID: {PAGE_ID} ---")
print("Testing with period='day' (Required for your time-series pipeline)\n")

# --- LIST OF METRICS TO TEST ---
# Add or remove metrics here to test them
metrics_to_test = [
    'page_impressions',
    'page_post_engagements',
    'page_fan_adds',        # New likes
    'page_fans',            # Total likes (Usually fails with period='day')
    'page_views_total',     # Requires 'pages_read_engagement' permission
    'page_engaged_users',
    'page_video_views',
    'page_actions_post_reactions_like_total'
]

valid_metrics = []

# --- THE TESTING LOOP ---
for metric in metrics_to_test:
    url = f"https://graph.facebook.com/v19.0/{PAGE_ID}/insights"
    
    params = {
        'metric': metric,
        'period': 'day',  # We strictly test 'day' because your BigQuery table is daily
        'access_token': ACCESS_TOKEN
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        print(f"✅ PASSED: {metric}")
        valid_metrics.append(metric)
    else:
        # Parse the error to give a useful hint
        error_data = response.json()
        error_msg = error_data.get('error', {}).get('message', 'Unknown Error')
        
        print(f"❌ FAILED: {metric}")
        print(f"   Reason: {error_msg}")

# --- SUMMARY ---
print("\n" + "="*40)
print("RESULTS SUMMARY")
print("="*40)

if valid_metrics:
    print("Copy this list into your ingest.py script:\n")
    # Formats the list nicely for Python
    formatted_list = "metrics = [\n    '" + "',\n    '".join(valid_metrics) + "'\n]"
    print(formatted_list)
else:
    print("No valid metrics found. Check your Access Token permissions.")