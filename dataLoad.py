import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from multiprocessing import Process
# import os
from dotenv import load_dotenv

load_dotenv()

### Table Creation Method:
def createTables():
    try:
        conn = psycopg2.connect("host=database dbname=dataEngineering user=postgres password=Suren@19_2004")
        cur = conn.cursor()
        with open('./dataSchema.sql', 'r') as file:
            dataSchema = file.read()
            cur.execute(dataSchema)
        conn.commit()
        conn.close()
    except psycopg2.OperationalError as e:
        print(f"Error creating tables: {e}")

### LoadData Method:
def loadData(tableName,df,colNames=0):
    flag=False
    while not flag:
        conn = psycopg2.connect("host=database dbname=dataEngineering user=postgres password=Suren@19_2004")
        engine = create_engine("postgresql://postgres:Suren%4019_2004@localhost:5432/dataEngineering")

        if colNames!=0:
            df = df.rename(columns=colNames)
        try:
            df.to_sql(name=tableName, con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
            print('done!')
            conn.commit()
            conn.close()
            flag=True
        except psycopg2.OperationalError as e:
            print(f"Error loading data into table {tableName}: {e}")
            print("Retrying...")


### Main Method : 
if __name__ == '__main__': 

    ### create tables :
    print('Creating Tables....')
    createTables()
    print('Tables created!')

    ## Genre file path :
    dfGenre = pd.read_csv('./genreList.csv')
    ## Movies file path :
    df = pd.read_csv('./filteredMoviesList.csv')

    ## moviesData :
    moviesData = df[['id','title', 'overview','popularity','release_date','vote_average','vote_count']]
    moviesDataCols = {'id':'movieId','release_date': 'releaseDate', 'vote_average': 'rating','vote_count':'voteCount'}

    ## movieFiles :
    moviesFiles = df[['id','poster_path', 'backdrop_path']]
    moviesFilesCols = {'id':'movieId','poster_path': 'posterPath', 'backdrop_path': 'backdropPath'}

    ## ogMoviesData
    ogMoviesData = df[['id','adult','original_language', 'original_title']]
    ogMoviesDataCols = {'id':'movieId','adult': 'isAdult', 'original_language': 'ogLanguage','original_title':'ogTitle'}

    ## movieGenres 
    movieGenres = df[['genre_ids','id']]
    movieGenresCols = {'id':'movieId','genre_ids':'genreId'}

    processes = []
    ### Load Genre :
    p1 = Process(target=loadData,args=('Genres',dfGenre))
    processes.append(p1)
    p1.start()
    print('started p1')

    ### Load moviesData :
    p2 = Process(target=loadData,args=('moviesData',moviesData,moviesDataCols))
    processes.append(p2)
    p2.start()
    print('started p2')

    ### Load moviesFiles :
    p3 = Process(target=loadData,args=('moviesFiles',moviesFiles,moviesFilesCols))
    processes.append(p3)
    p3.start()
    print('started p3')

    ### Load moviesGenres :
    p4 = Process(target=loadData,args=('movieGenres',movieGenres,movieGenresCols))
    processes.append(p4)
    p4.start()
    print('started p4')

    ### Load ogMoviesData :
    p5 = Process(target=loadData,args=('ogMoviesData',ogMoviesData,ogMoviesDataCols))
    processes.append(p5)
    p5.start()
    print('started p5')

    ### join all 
    for p in processes:
        p.join()


