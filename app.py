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

### --------------> #TODO# Return Array of Objects:

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

### --------------> #TODO# Returns Array of Arrays (df):

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

### moviesFiles getAll APIs:

@app.route('/api/data/moviesFiles/all', methods=['GET'])
def getDataAllMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."posterPath",mf."backdropPath" from "moviesFiles" as mf 
Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesFiles/all/poster', methods=['GET'])
def getDataPosterMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."posterPath" from "moviesFiles" as mf 
Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesFiles/all/backdrop', methods=['GET'])
def getDataBackdropMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT mf."movieId",md.title,mf."backdropPath" from "moviesFiles" as mf 
Inner Join "moviesData" as md on mf."movieId" = md."movieId" ORDER BY "movieId" ASC')
  records = cur.fetchall()
  return jsonify(records)

@app.route('/api/data/moviesFiles/all/raw', methods=['GET'])
def getDataRawAllMoviesFiles():
  conn = psycopg2.connect(f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}")
  cur = conn.cursor()
  cur.execute(f'SELECT * FROM "moviesFiles" ORDER BY id ASC')
  records = cur.fetchall()
  return jsonify(records)



if __name__ == '__main__':
  app.run(port=5000)