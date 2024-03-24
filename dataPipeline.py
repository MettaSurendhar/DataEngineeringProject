import requests
import json
response_API = requests.get()

data = response_API.text
print(data)
# parse_json = json.loads(data)
# print(parse_json)
# 'https://api.covid19india.org/state_district_wise.json'