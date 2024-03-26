import json
from flask import Flask,jsonify,request
import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

load_dotenv()

app = Flask(__name__)

@app.route('/', methods=['GET'])
@app.route('/api',methods=['GET'])
def get_init():
  return '<center><h1 style="margin:0;padding:0;text-align:center;margin-top:45vh;font-size:48px;">Welcome to Metta&#39s Movie API<h1/><center/>'

apiList = []
with open('./data.json', 'r') as file:
  apiList.append(json.load(file))

@app.route('/api/all',methods=['GET'])
def getAllAPI():
  return jsonify(apiList)

#?--------------------------------------------------------------------------------
### --------------> #TODO# Return Array of Objects:
#?--------------------------------------------------------------------------------

### Genre getAll APIs:
@app.route('/api/json/genres/all', methods=['GET'])
def getJsonAllGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT name FROM "Genres" ORDER BY id')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/genres/all/raw', methods=['GET'])
def getJsonRawAllGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT * FROM "Genres" ORDER BY id')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### Genre one APIs:

@app.route('/api/json/genre/id<int:id>', methods=['GET'])
def getJsonAGenres(id:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT name FROM "Genres" where id={id}')
  record = cur.fetchone()
  response = dict(record)
  return jsonify(response)

### Genre getAll limit APIs:
@app.route('/api/json/genres/all/limit/<int:limit>', methods=['GET'])
def getJsonAllGenresLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT name FROM "Genres" ORDER BY id LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/genres/all/raw/limit/<int:limit>', methods=['GET'])
def getJsonRawAllGenresLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT * FROM "Genres" ORDER BY id LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### moviesData getAll APIs:
@app.route('/api/json/movies/all', methods=['GET'])
def getJsonAllMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY id')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/raw', methods=['GET'])
def getJsonRawAllMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT * FROM "moviesData" ORDER BY id')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/topRated', methods=['GET'])
def getJsonTopRatedMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY rating')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/popular', methods=['GET'])
def getJsonPopularMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY popularity')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/mostVoted', methods=['GET'])
def getJsonMostVotedMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY "voteCount"')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/recent', methods=['GET'])
def getJsonRecentMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY "releaseDate" DESC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/old', methods=['GET'])
def getJsonOldMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY "releaseDate" ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/trending', methods=['GET'])
def getJsonTrendingMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY (popularity+rating)/2,"releaseDate" DESC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### moviesData get one value APIs:

@app.route('/api/json/movie/id/<int:id>', methods=['GET'])
def getJsonMovie(id:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" WHERE "movieId"={id}')
  record = cur.fetchone()
  response = dict(record) 
  return jsonify(response)

@app.route('/api/json/movie/raw/id/<int:id>', methods=['GET'])
def getJsonRawMovie(id:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT * FROM "moviesData" WHERE "movieId"={id}')
  record = cur.fetchone()
  response = dict(record) 
  return jsonify(response)

### moviesData getAll limit APIs:
@app.route('/api/json/movies/all/limit/<int:limit>', methods=['GET'])
def getJsonAllMoviesLimit(limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY id LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/raw/limit/<int:limit>', methods=['GET'])
def getJsonRawAllMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT * FROM "moviesData" ORDER BY id LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/topRated/limit/<int:limit>', methods=['GET'])
def getJsonTopRatedMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY rating LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/popular/limit/<int:limit>', methods=['GET'])
def getJsonPopularMoviesLimit(limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY popularity LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/mostVoted/limit/<int:limit>', methods=['GET'])
def getJsonMostVotedMoviesLimit(limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY "voteCount" limit {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/recent/limit/<int:limit>', methods=['GET'])
def getJsonRecentMoviesLimit(limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY "releaseDate" DESC limit {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/old/limit/<int:limit>', methods=['GET'])
def getJsonOldMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY "releaseDate" ASC limit {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movies/all/trending/limit/<int:limit>', methods=['GET'])
def getJsonTrendingMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY (popularity+rating)/2,"releaseDate" DESC limit {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### moviesFiles getAll APIs:

@app.route('/api/json/moviesFiles/all', methods=['GET'])
def getJsonAllMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."backdropPath",mf."posterPath from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/moviesFiles/all/raw', methods=['GET'])
def getJsonRawAllMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT * FROM "moviesFiles" ORDER BY id ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/moviesFiles/all/poster', methods=['GET'])
def getJsonPosterMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."posterPath from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/moviesFiles/all/backdrop', methods=['GET'])
def getJsonBackdropMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."backdropPath" from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### moviesFiles getAll limit APIs:

@app.route('/api/json/moviesFiles/all/limit/<int:limit>', methods=['GET'])
def getJsonAllMoviesFilesLimit(limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."backdropPath",mf."posterPath from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/moviesFiles/all/raw/limit/<int:limit>', methods=['GET'])
def getJsonRawAllMoviesFilesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT * FROM "moviesFiles" ORDER BY id ASC LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/moviesFiles/all/poster/limit/<int:limit>', methods=['GET'])
def getJsonPosterMoviesFilesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."posterPath from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/moviesFiles/all/backdrop/limit/<int:limit>', methods=['GET'])
def getJsonBackdropMoviesFilesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."backdropPath" from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### ogMoviesData getAll APIs:

@app.route('/api/json/ogMoviesData/all', methods=['GET'])
def getJsonAllOgMoviesData():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogTitle",mf."ogLanguage",mf."isAdult" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/ogMoviesData/all/raw', methods=['GET'])
def getJsonRawAllOgMoviesData():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT * FROM "ogMoviesData" ')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/ogMoviesData/all/title', methods=['GET'])
def getJsonTitleOgMoviesData():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogTitle" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/ogMoviesData/all/language', methods=['GET'])
def getJsonLanguageOgMoviesData():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogLanguage" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### ogMoviesData getAll limit APIs:

@app.route('/api/json/ogMoviesData/all/limit/<int:limit>', methods=['GET'])
def getJsonAllOgMoviesDataLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogTitle",mf."ogLanguage",mf."isAdult" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/ogMoviesData/all/raw/limit/<int:limit>', methods=['GET'])
def getJsonRawAllOgMoviesDataLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT * FROM "ogMoviesData" LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/ogMoviesData/all/title/limit/<int:limit>', methods=['GET'])
def getJsonTitleOgMoviesDataLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogTitle" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/ogMoviesData/all/language/limit/<int:limit>', methods=['GET'])
def getJsonLanguageOgMoviesDataLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogLanguage" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### movieGenres getAll APIs:

@app.route('/api/json/movieGenres/all', methods=['GET'])
def getJsonAllMovieGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mg."movieId",md.title, ge.name   from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movieGenres/all/raw', methods=['GET'])
def getJsonRawAllMovieGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT *  from  "movieGenres" ')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### movieGenres getAll limit APIs:

@app.route('/api/json/movieGenres/all/limit/<int:limit>', methods=['GET'])
def getJsonAllMovieGenresLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mg."movieId",md.title, ge.name   from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" ORDER BY "movieId" ASC limit {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movieGenres/all/raw/limit/<int:limit>', methods=['GET'])
def getJsonRawAllMovieGenresLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT *  from  "movieGenres" limit {limit} ')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### moviesGenres filtering APIs:

@app.route('/api/json/movieGenres/count/movies', methods=['GET'])
def getJsonMoviesCountMoviesGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT ge."name" as "Genre",COUNT(*) as "moviesCount"  from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" GROUP BY ge."name" ORDER BY "moviesCount" DESC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movieGenres/popular/movies', methods=['GET'])
def getJsonMoviesPopularMoviesGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT ge."name" as "Genre",(Select md2."title" from "moviesData" as md2 Inner Join "movieGenres" as mg2 on md2."movieId" = mg2."movieId" Where mg2."genreId" = ge."id" Order by md2."popularity" DESC Limit 1) as "popularMovie"  from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" GROUP BY ge."id",ge."name" ORDER BY "Genre" ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

### movieGenres getAll genreId APIs:

@app.route('/api/json/movieGenres/genre/<int:id>', methods=['GET'])
def getJsonGenreIdMovieGenres(id:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mg."movieId",md.title,md.overview,md."releaseDate"   from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" WHERE mg."genreId"={id} ORDER BY "movieId" ASC')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

@app.route('/api/json/movieGenres/count/genre/<int:id>', methods=['GET'])
def getJsonGenreIdCountMovieGenres(id:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT COUNT(*)   from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" WHERE mg."genreId"={id}')
  records = cur.fetchone()
  response = [dict(record) for record in records]
  return jsonify(response)

### movieGenres getAll genreId limit APIs:

@app.route('/api/json/movieGenres/genre/<int:id>/limit/<int:limit>', methods=['GET'])
def getJsonGenreIdMovieGenresLimit(id:int,limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor(cursor_factory=DictCursor)
  cur.execute(f'SELECT mg."movieId",md.title,md.overview,md."releaseDate"   from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" WHERE mg."genreId"={id} ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  response = [dict(record) for record in records]
  return jsonify(response)

#?-----------------------------------------------------------------------------------
### --------------> #TODO# Returns Array of Arrays (df):
#?------------------------------------------------------------------------------------

### Genre getAll APIs:
@app.route('/api/data/genres/all', methods=['GET'])
def getDataAllGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT name FROM "Genres" ORDER BY id')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/genres/all/raw', methods=['GET'])
def getDataRawAllGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * FROM "Genres" ORDER BY id')
  records = cur.fetchall()
  return jsonify(records)

### Genre one APIs:
@app.route('/api/data/genre/id/<int:id>', methods=['GET'])
def getDataGenre(id:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT name FROM "Genres" where id={id}')
  record = cur.fetchone()
  return jsonify(record)

### Genre getAll limit APIs:
@app.route('/api/data/genres/all/limit/<int:limit>', methods=['GET'])
def getDataAllGenresLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT name FROM "Genres" ORDER BY id LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/genres/all/raw/limit/<int:limit>', methods=['GET'])
def getDataRawAllGenresLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * FROM "Genres" ORDER BY id LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

### moviesData getAll APIs :
@app.route('/api/data/movies/all', methods=['GET'])
def getDataAllMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY id')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/raw', methods=['GET'])
def getDataRawAllMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * FROM "moviesData" ORDER BY id')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/topRated', methods=['GET'])
def getDataTopRatedMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY rating')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/popular', methods=['GET'])
def getDataPopularMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY popularity')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/mostVoted', methods=['GET'])
def getDataMostVotedMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY voteCount')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/recent', methods=['GET'])
def getDataRecentMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY "releaseDate" DESC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/old', methods=['GET'])
def getDataOldMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY "releaseDate" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/trending', methods=['GET'])
def getDataTrendingMovies():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY (popularity+rating)/2,"releaseDate" DESC')
  records = cur.fetchall()
  return jsonify(records)

### moviesData get one value APIs:

@app.route('/api/data/movie/id/<int:id>', methods=['GET'])
def getDataMovie(id:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" WHERE "movieId"={id}')
  record = cur.fetchone()
  return jsonify(record)

@app.route('/api/data/movie/raw/id/<int:id>', methods=['GET'])
def getDataRawMovie(id:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * FROM "moviesData" WHERE "movieId"={id}')
  record = cur.fetchone()
  return jsonify(record)


### moviesData getAll limit APIs :
@app.route('/api/data/movies/all/limit/<int:limit>', methods=['GET'])
def getDataAllMoviesLimit(limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY id limit {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/raw/limit/<int:limit>', methods=['GET'])
def getDataRawAllMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * FROM "moviesData" ORDER BY id limit {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/topRated/limit/<int:limit>', methods=['GET'])
def getDataTopRatedMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY rating limit {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/popular/limit/<int:limit>', methods=['GET'])
def getDataPopularMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY popularity limit {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/mostVoted/limit/<int:limit>', methods=['GET'])
def getDataMostVotedMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY voteCount limit {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/recent/limit/<int:limit>', methods=['GET'])
def getDataRecentMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY "releaseDate" DESC limit {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/old/limit/<int:limit>', methods=['GET'])
def getDataOldMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY "releaseDate" ASC limit {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/movies/all/trending/limit/<int:limit>', methods=['GET'])
def getDataTrendingMoviesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT "movieId",title,overview,"releaseDate" FROM "moviesData" ORDER BY (popularity+rating)/2,"releaseDate" DESC limit {limit}')
  records = cur.fetchall()
  return jsonify(records)

### moviesFiles getAll APIs:

@app.route('/api/data/moviesFiles/all', methods=['GET'])
def getDataAllMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute('SELECT mf."movieId",md.title,mf."posterPath",mf."backdropPath" from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesFiles/all/poster', methods=['GET'])
def getDataPosterMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."posterPath" from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesFiles/all/backdrop', methods=['GET'])
def getDataBackdropMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."backdropPath" from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesFiles/all/raw', methods=['GET'])
def getDataRawAllMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * FROM "moviesFiles" ORDER BY id ASC')
  records = cur.fetchall()
  return jsonify(records)

### moviesFiles getAll limit APIs:

@app.route('/api/data/moviesFiles/all/limit/<int:limit>', methods=['GET'])
def getDataAllMoviesFilesLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."posterPath",mf."backdropPath" from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesFiles/all/poster/limit/<int:limit>', methods=['GET'])
def getDataPosterMoviesFilesLimit(limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."posterPath" from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesFiles/all/backdrop/limit/<int:limit>', methods=['GET'])
def getDataBackdropMoviesFilesLimit(limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."backdropPath" from "moviesFiles" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesFiles/all/raw/limit/<int:limit>', methods=['GET'])
def getDataRawAllMoviesFilesLimit(limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * FROM "moviesFiles" ORDER BY id ASC LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

### ogMoviesData getAll APIs:

@app.route('/api/data/ogMoviesData/all', methods=['GET'])
def getDataAllOgMoviesData():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogTitle",mf."ogLanguage",mf."isAdult" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/ogMoviesData/all/raw', methods=['GET'])
def getDataRawAllOgMoviesData():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * from "ogMoviesData"')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/ogMoviesData/all/title', methods=['GET'])
def getDataTitleOgMoviesData():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogTitle" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/ogMoviesData/all/language', methods=['GET'])
def getDataLanguageOgMoviesData():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogLanguage" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

### ogMoviesData getAll limit APIs:

@app.route('/api/data/ogMoviesData/all/limit/<int:limit>', methods=['GET'])
def getDataAllOgMoviesDataLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogTitle",mf."ogLanguage",mf."isAdult" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/ogMoviesData/all/raw/limit/<int:limit>', methods=['GET'])
def getDataRawAllOgMoviesDataLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * from "ogMoviesData" LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/ogMoviesData/all/title/limit/<int:limit>', methods=['GET'])
def getDataTitleOgMoviesDataLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogTitle" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/ogMoviesData/all/language/limit/<int:limit>', methods=['GET'])
def getDataLanguageOgMoviesDataLimit(limit: int):  
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."ogLanguage" from "ogMoviesData" as mf Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

### moviesGenres getAll APIs:

@app.route('/api/data/moviesGenres/all', methods=['GET'])
def getDataAllMoviesGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mg."movieId",md.title, ge.name   from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesGenres/all/raw', methods=['GET'])
def getDataRawAllMoviesGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * from  "movieGenres" ')
  records = cur.fetchall()
  return jsonify(records)

### moviesGenres getAll limit APIs:

@app.route('/api/data/moviesGenres/all/limit/<int:limit>', methods=['GET'])
def getDataAllMoviesGenresLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mg."movieId",md.title, ge.name   from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" ORDER BY "movieId" ASC LIMIT {limit}')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesGenres/all/raw/limit/<int:limit>', methods=['GET'])
def getDataRawAllMoviesGenresLimit(limit: int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * from  "movieGenres" LIMIT {limit} ')
  records = cur.fetchall()
  return jsonify(records)


### moviesGenres getAll genreId APIs:

@app.route('/api/data/moviesGenres/genre/<int:id>', methods=['GET'])
def getDataGenreIdMoviesGenres(id:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mg."movieId",md.title,md.overview,md."releaseDate"  from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" WHERE mg."genreId" = {id} ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesGenres/count/genre/<int:id>', methods=['GET'])
def getDataGenreIdCountMoviesGenres(id:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT COUNT(*) from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" WHERE mg."genreId" = {id}')
  records = cur.fetchone()
  return jsonify(records)

### moviesGenres getAll genreId limit APIs:

@app.route('/api/data/moviesGenres/genre/<int:id>/limit/<int:limit>', methods=['GET'])
def getDataGenreIdMoviesGenresLimit(id:int,limit:int):
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mg."movieId",md.title,md.overview,md."releaseDate"   from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" WHERE mg."genreId" = {id} ORDER BY "movieId" ASC Limit {limit}')
  records = cur.fetchall()
  return jsonify(records)

### moviesGenres filtering APIs:

@app.route('/api/data/moviesGenres/count/movies', methods=['GET'])
def getDataMoviesCountMoviesGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT ge."name" as "Genre",COUNT(*) as "moviesCount"  from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" GROUP BY ge."name" ORDER BY "moviesCount" DESC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesGenres/popular/movies', methods=['GET'])
def getDataMoviesPopularMoviesGenres():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT ge."name" as "Genre",(Select md2."title" from "moviesData" as md2 Inner Join "movieGenres" as mg2 on md2."movieId" = mg2."movieId" Where mg2."genreId" = ge."id" Order by md2."popularity" DESC Limit 1) as "popularMovie"  from  "movieGenres" as mg Inner Join "moviesData" as md on mg."movieId" = md."movieId" Inner Join "Genres" as ge on mg."genreId" = ge."id" GROUP BY ge."id",ge."name" ORDER BY "Genre" ASC')
  records = cur.fetchall()
  return jsonify(records)

if __name__ == '__main__':
  app.run(port=5000)