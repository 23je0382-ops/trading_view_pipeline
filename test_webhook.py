import requests

url = "http://localhost:5001/webhook"
data = {
    "ticker": "AAPL",
    "time": "2026-05-01T15:30:00Z",
    "open": 150.5,
    "high": 152.0,
    "low": 149.0,
    "close": 151.2,
    "volume": 1000000
}

try:
    print(f"Sending test payload to {url}...")
    response = requests.post(url, json=data)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
except requests.exceptions.ConnectionError:
    print("Error: Failed to connect. Please ensure the Flask server is running (python app.py).")
