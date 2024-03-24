# import pandas as pd
# import requests
# import json
# response_API = requests.get('https://api.covid19india.org/state_district_wise.json')

# data = response_API.text
# print(data)
# df = pd.DataFrame(response_API.json())
# print(df)
# parse_json = json.loads(data)
# print(parse_json)
# 'https://api.covid19india.org/state_district_wise.json'


#TODO Genre list : 
# url = "https://api.themoviedb.org/3/genre/movie/list?language=en"

# headers = {
#     "accept": "application/json",
#     "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4MWY5YzYzNGMwMWIyZjA4ZmI0MDNkZWExNzdiOTIyNSIsInN1YiI6IjY1ZmU5ZTc2MDQ3MzNmMDE3ZGVjNjBmNCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.mvfphI1NIKujcbUUTZ0-dX9pN0sefVmbdj3uwMEHsj0"
# }

# response = requests.get(url, headers=headers)

# print(response.text)

#TODO popular movie list : 
#? page 1 - 500 
#** Things have to see : popularity(popular) , vote average(top rated) , genre_ids(genre) , original_language(lang) , release_date , title , og title ,
# url = "https://api.themoviedb.org/3/movie/popular?language=en-US&page=1"

# headers = {
#     "accept": "application/json",
#     "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4MWY5YzYzNGMwMWIyZjA4ZmI0MDNkZWExNzdiOTIyNSIsInN1YiI6IjY1ZmU5ZTc2MDQ3MzNmMDE3ZGVjNjBmNCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.mvfphI1NIKujcbUUTZ0-dX9pN0sefVmbdj3uwMEHsj0"
# }

# response = requests.get(url, headers=headers)

# print(response.text)

import requests
import json
import csv
import time

page=1
headers = {
      "accept": "application/json",
      "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4MWY5YzYzNGMwMWIyZjA4ZmI0MDNkZWExNzdiOTIyNSIsInN1YiI6IjY1ZmU5ZTc2MDQ3MzNmMDE3ZGVjNjBmNCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.mvfphI1NIKujcbUUTZ0-dX9pN0sefVmbdj3uwMEHsj0"
  }

data_file = open('./database.csv', 'a', encoding='utf-8', newline='')

while page<=500:
  print(page)
  url = f"https://api.themoviedb.org/3/movie/popular?language=en-US&page={page}"

  try:
    response = requests.get(url, headers=headers)
    json_data = response.json()['results']
    print(json_data)

    fieldnames = json_data[0].keys()
    csv_writer = csv.DictWriter(data_file, fieldnames=fieldnames)
    if page==1:
      csv_writer.writeheader()
    
    for val in json_data: 
        csv_writer.writerow(val)
    page=page+1
  except:
     print('error fetching')
     time.sleep(1)
     page=page
  
data_file.close()
  

# print(response.text)

# df = pd.DataFrame(response.json())
# print(df)
