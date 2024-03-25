import psycopg2
import pandas as pd
from sqlalchemy import create_engine


conn = psycopg2.connect("host=localhost dbname=dataEngineering user=postgres password=Suren@19_2004")
cur = conn.cursor()
engine = create_engine('postgresql://postgres:Suren%4019_2004@localhost:5432/dataEngineering')

### create tables :

# cur.execute("""

# BEGIN;


# CREATE TABLE IF NOT EXISTS public."Genres"
# (
#     id integer NOT NULL,
#     name text COLLATE pg_catalog."default" NOT NULL,
#     CONSTRAINT "Genres_pkey" PRIMARY KEY (id)
# );

# CREATE TABLE IF NOT EXISTS public."moviesFiles"
# (
#     id serial NOT NULL,
#     "movieId" integer NOT NULL,
#     "posterPath" text COLLATE pg_catalog."default",
#     "backdropPath" text COLLATE pg_catalog."default",
#     CONSTRAINT files_pkey PRIMARY KEY (id)
# );

# CREATE TABLE IF NOT EXISTS public."movieGenres"
# (
#     id serial NOT NULL,
#     "movieId" integer NOT NULL,
#     "genreId" integer NOT NULL,
#     CONSTRAINT "movieGenres_pkey" PRIMARY KEY (id)
# );

# CREATE TABLE IF NOT EXISTS public."moviesData"
# (
#     id serial NOT NULL,
#     "movieId" integer NOT NULL,
#     title text COLLATE pg_catalog."default" NOT NULL,
#     overview text COLLATE pg_catalog."default",
#     popularity double precision NOT NULL,
#     "releaseDate" date,
#     rating double precision NOT NULL,
#     "voteCount" integer,
#     CONSTRAINT "moviesData_pkey" PRIMARY KEY (id),
#     CONSTRAINT "movieId" UNIQUE ("movieId")
# );

# CREATE TABLE IF NOT EXISTS public."ogMoviesData"
# (
#     id serial NOT NULL,
#     "isAdult" boolean,
#     "ogLanguage" text COLLATE pg_catalog."default",
#     "ogTitle" text COLLATE pg_catalog."default",
#     "movieId" integer NOT NULL,
#     CONSTRAINT "ogMoviesData_pkey" PRIMARY KEY (id)
# );

# ALTER TABLE IF EXISTS public."moviesFiles"
#     ADD CONSTRAINT "movieId" FOREIGN KEY ("movieId")
#     REFERENCES public."moviesData" ("movieId") MATCH SIMPLE
#     ON UPDATE NO ACTION
#     ON DELETE NO ACTION
#     NOT VALID;


# ALTER TABLE IF EXISTS public."movieGenres"
#     ADD CONSTRAINT "genreId" FOREIGN KEY ("genreId")
#     REFERENCES public."Genres" (id) MATCH SIMPLE
#     ON UPDATE NO ACTION
#     ON DELETE NO ACTION
#     NOT VALID;


# ALTER TABLE IF EXISTS public."movieGenres"
#     ADD CONSTRAINT "movieId" FOREIGN KEY ("movieId")
#     REFERENCES public."moviesData" ("movieId") MATCH SIMPLE
#     ON UPDATE NO ACTION
#     ON DELETE NO ACTION
#     NOT VALID;


# ALTER TABLE IF EXISTS public."ogMoviesData"
#     ADD CONSTRAINT "movieId" FOREIGN KEY ("movieId")
#     REFERENCES public."moviesData" ("movieId") MATCH SIMPLE
#     ON UPDATE NO ACTION
#     ON DELETE NO ACTION
#     NOT VALID;

# END;

# """)

# conn.commit()

# print('created table')

# host=localhost dbname=dataEngineering user=postgres password=Suren@19_2004


### genre :

# df = pd.read_csv('./genreList.csv')
# print('read file')

# df.to_sql(name='Genres', con=engine, if_exists='append', index=False)
# print('done!')
# conn.commit()

### LoadData :

def loadData(df,colNames,tableName):
    df = df.rename(columns=colNames)

    df.to_sql(name=tableName, con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
    print('done!')
    conn.commit()

# ### Load moviesData
df = pd.read_csv('./moviesList.csv')

moviesData = df[['id','title', 'overview','popularity','release_date','vote_average','vote_count']]
moviesDataCols = {'id':'movieId','release_date': 'releaseDate', 'vote_average': 'rating','vote_count':'voteCount'}
loadData(moviesData,moviesDataCols,'moviesData')

# ### Load movieFiles
# moviesFiles = df[['id','poster_path', 'backdrop_path']]
# moviesFilesCols = {'id':'movieId','poster_path': 'posterPath', 'backdrop_path': 'backdropPath'}
# loadData(moviesFiles,moviesFilesCols,'moviesFiles')

# ### Load ogMoviesData
# ogMoviesData = df[['id','adult','original_language', 'original_title']]
# ogMoviesDataCols = {'id':'movieId','adult': 'isAdult', 'original_language': 'ogLanguage','original_title':'ogTitle'}
# loadData(ogMoviesData,ogMoviesDataCols,'ogMoviesData')

# ### Load movieGenres 
# df['genre_ids'] = df['genre_ids'].str.replace('[', '').str.replace(']', '').str.split(',').str[0]
# movieGenres = df[['genre_ids','id']]
# movieGenresCols = {'id':'movieId','genre_ids':'genreId'}
# loadData(movieGenres,movieGenresCols,'movieGenres')

conn.close()


