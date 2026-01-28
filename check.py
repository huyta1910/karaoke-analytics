import os
import requests
from dotenv import load_dotenv

load_dotenv()
ACCESS_TOKEN = os.getenv('FB_ACCESS_TOKEN')

# Check who owns this token
url = "https://graph.facebook.com/me"
params = {'access_token': ACCESS_TOKEN}
response = requests.get(url, params=params).json()

print("--- TOKEN CHECK ---")
print(f"ID:   {response.get('id')}")
print(f"Name: {response.get('name')}")
print("-------------------")