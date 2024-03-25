import requests
import csv
import time
from multiprocessing import Process
import os
from dotenv import load_dotenv

load_dotenv()

### Extract Genre Method : 
def genre_extract(genreFilePath,headers):
  flag=False
  while(flag!=True):
    print(flag,'genre')
    data_file = open(genreFilePath, 'a', encoding='utf-8', newline='')
    url = f"{os.getenv('GENRE_LIST_API')}"
    try:
      response = requests.get(url, headers=headers)
      json_data = response.json()['genres']

      fieldnames = json_data[0].keys()
      csv_writer = csv.DictWriter(data_file, fieldnames=fieldnames)
      csv_writer.writeheader()

      for val in json_data: 
          csv_writer.writerow(val)
      flag=True
    except Exception as e:
        print(f'error fetching,{e}')
        time.sleep(1)

    data_file.close()

### Extract Page1 Movies Method :
def extract_page1(filePath,headers):
  flag=False
  while(flag!=True):
    print(flag,'page1')
    data_file = open(filePath, 'a', encoding='utf-8', newline='')
    url = f"{os.getenv('MOVIE_LIST_API')}&page=1"
    try:
      response = requests.get(url, headers=headers)
      json_data = response.json()['results']

      fieldnames = json_data[0].keys()
      csv_writer = csv.DictWriter(data_file, fieldnames=fieldnames)
      csv_writer.writeheader()

      for val in json_data: 
          csv_writer.writerow(val)
      flag=True
    except Exception as e:
        print(f'error fetching,{e}')

    data_file.close()


### Extract Movies Method :
def extract_api(start,end,headers,filePath):
  data_file = open(filePath, 'a', encoding='utf-8', newline='')
  page=start
  while page<=end:
    print(page)
    url = f"{os.getenv('MOVIE_LIST_API')}&page={page}"

    try:
      response = requests.get(url, headers=headers)
      json_data = response.json()['results']

      fieldnames = json_data[0].keys()
      csv_writer = csv.DictWriter(data_file, fieldnames=fieldnames)
      if page==1:
        csv_writer.writeheader()
      
      for val in json_data: 
          csv_writer.writerow(val)
      page=page+1
    except Exception as e:
      print(f'error fetching,{e}')
      time.sleep(1)
      page=page
  data_file.close()


### Main Method : 
if __name__ == '__main__': 

  headers = {
      "accept": "application/json",
      "Authorization": f"Bearer {os.getenv('API_KEY')}"
  }
  moviesFilePath='./moviesList.csv'
  genreFilePath='./genreList.csv'

  ### genre :
  genre_extract(genreFilePath,headers)

  ###  page 1 : 
  extract_page1(moviesFilePath,headers)
  
  ### all pages :
  processes = []

  p1 = Process(target=extract_api,args=(2,100,headers,moviesFilePath))
  processes.append(p1)
  p1.start()
  for val in [101,201,301,401]:
    p = Process(target=extract_api,args=(val,99+val,headers,moviesFilePath))
    processes.append(p)
    p.start()
  for p in processes:
    p.join()

