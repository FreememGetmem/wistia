import requests


# Wistia API Configuration
API_TOKEN = "0323ade64e13f79821bdc0f2a9410d9ec3873aa9df01f8a4a54d4e0f3dd2e6b4"  
MEDIA_ID = "v08dlrgr7v"  # The given media ID


# Wistia Stats API Endpoint
url = f"https://api.wistia.com/v1/stats/medias/{MEDIA_ID}.json"


# API Headers
headers = {
    "Authorization": f"Bearer {API_TOKEN}"
}


# Make API Request
response = requests.get(url, headers=headers)


# Handle Response
if response.status_code == 200:
    stats = response.json()
    print("✅ Video Stats Retrieved Successfully:\n")
    print(stats)
elif response.status_code == 401:
    print("❌ Unauthorized: Check your API token permissions.")
elif response.status_code == 404:
    print("❌ Error: Media not found. Check if the media ID is correct.")
else:
    print(f"⚠️ Error: Received status code {response.status_code} - {response.text}")
