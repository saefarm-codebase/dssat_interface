import requests
import json

url = 'http://127.0.0.1:8000/Barley'

params = {'xcoord': 126.557727,
          'ycoord': 36.3863365}

response = requests.get(url, json=params)
response = json.loads(response.content.decode('utf-8'))
print(json.dumps(response, indent=3))
