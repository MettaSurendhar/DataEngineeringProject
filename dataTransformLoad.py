import psycopg2
import pandas as pd
from sqlalchemy import create_engine


conn = psycopg2.connect("host=localhost dbname=dataEngineering user=postgres password=Suren@19_2004")
cur = conn.cursor()
engine = create_engine('postgresql://postgres:Suren%4019_2004@localhost:5432/dataEngineering')

### create tables :

cur.execute("""
    BEGIN;


DROP TABLE IF EXISTS public."Genres";

CREATE TABLE IF NOT EXISTS public."Genres"
(
    id integer NOT NULL,
    name text NOT NULL,
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS public."moviesData";

CREATE TABLE IF NOT EXISTS public."moviesData"
(
    id serial NOT NULL,
    "movieId" integer NOT NULL,
    title text NOT NULL,
    overview text,
    popularity float,
    "releaseDate" date,
    rating float,
    "voteCount" integer,
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS public.files;

CREATE TABLE IF NOT EXISTS public.files
(
    id serial NOT NULL,
    "movieId" integer NOT NULL,
    "posterPath" text,
    "backdropPath" text,
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS public."ogMoviesData";

CREATE TABLE IF NOT EXISTS public."ogMoviesData"
(
    id serial NOT NULL,
    "isAdult" boolean,
    "ogLanguage" text,
    ogtitle text,
    "movieId" integer NOT NULL,
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS public."movieGenres";

CREATE TABLE IF NOT EXISTS public."movieGenres"
(
    id serial NOT NULL,
    "movieId" integer NOT NULL,
    "genreId" integer NOT NULL,
    PRIMARY KEY (id)
);

ALTER TABLE IF EXISTS public.files
    ADD CONSTRAINT "movieId" FOREIGN KEY ("movieId")
    REFERENCES public."moviesData" (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."ogMoviesData"
    ADD CONSTRAINT "movieId" FOREIGN KEY ("movieId")
    REFERENCES public."moviesData" (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."movieGenres"
    ADD CONSTRAINT "genreId" FOREIGN KEY ("genreId")
    REFERENCES public."Genres" (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."movieGenres"
    ADD CONSTRAINT "movieId" FOREIGN KEY ("movieId")
    REFERENCES public."moviesData" (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;

""")

conn.commit()

print('created table')

# host=localhost dbname=dataEngineering user=postgres password=Suren@19_2004


### genre :

df = pd.read_csv('./genreList.csv')
print('read file')

df.to_sql(name='Genres', con=engine, if_exists='append', index=False)
print('done!')
conn.commit()

### moviesData :

df = pd.read_csv('./moviesList.csv')
moviesData = df[['id','title', 'overview','popularity','release_date','vote_average','vote_count']]
column_names = {'id':'movieId','release_date': 'releaseDate', 'vote_average': 'rating','vote_count':'voteCount'}
moviesData = moviesData.rename(columns=column_names)

moviesData.to_sql(name='moviesData', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
print('done!')
conn.commit()

conn.close()
# Insert data into the database

