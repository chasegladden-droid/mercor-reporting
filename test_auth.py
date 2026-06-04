import os
import requests
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("SPROUT_CLIENT_ID")
client_secret = os.getenv("SPROUT_CLIENT_SECRET")

response = requests.post(
    "https://api.sproutsocial.com/oauth/token",
    data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
)

if response.status_code == 200:
    token = response.json().get("access_token")
    print(f"✓ Auth successful. Token: {token[:20]}...")
else:
    print(f"✗ Auth failed: {response.status_code}")
    print(response.text)
