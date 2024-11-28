import requests
import json

# You need to add an item_id to match the FastAPI endpoint structure
item_id = 1  # This is the item ID you want to update, adjust as needed

# URL should include the item_id in the path
url = f"http://127.0.0.1:8000/items/{item_id}"

# Headers for JSON requests
headers = {
    "accept": "application/json",
}

# This should match the structure of the `Item` model from your FastAPI code
data = {
    "name": "Test Item",
    "price": 99.99,
    "is_offer": True  # Can also be None or False
}

# Make the POST request to the FastAPI server
# Instead of `data`, use `json` to automatically convert the dictionary to JSON
response = requests.post(url, headers=headers, json=data)

# Output the server's response
print(response.json())