BEGIN;


CREATE TABLE IF NOT EXISTS public."Genres"
(
    id integer NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT "Genres_pkey" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."moviesFiles"
(
    id serial NOT NULL,
    "movieId" integer NOT NULL,
    "posterPath" text COLLATE pg_catalog."default",
    "backdropPath" text COLLATE pg_catalog."default",
    CONSTRAINT "moviesFiles_pkey" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."movieGenres"
(
    id serial NOT NULL,
    "movieId" integer NOT NULL,
    "genreId" integer NOT NULL,
    CONSTRAINT "movieGenres_pkey" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."moviesData"
(
    id serial NOT NULL,
    "movieId" integer NOT NULL UNIQUE,
    title text COLLATE pg_catalog."default" NOT NULL,
    overview text COLLATE pg_catalog."default",
    popularity double precision NOT NULL,
    "releaseDate" date,
    rating double precision NOT NULL,
    "voteCount" integer,
    CONSTRAINT "moviesData_pkey" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."ogMoviesData"
(
    id serial NOT NULL,
    "isAdult" boolean,
    "ogLanguage" text COLLATE pg_catalog."default",
    "ogTitle" text COLLATE pg_catalog."default",
    "movieId" integer NOT NULL,
    CONSTRAINT "ogMoviesData_pkey" PRIMARY KEY (id)
);

ALTER TABLE IF EXISTS public."moviesFiles"
    ADD CONSTRAINT "movieId" FOREIGN KEY ("movieId")
    REFERENCES public."moviesData" ("movieId") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE;


ALTER TABLE IF EXISTS public."movieGenres"
    ADD CONSTRAINT "genreId" FOREIGN KEY ("genreId")
    REFERENCES public."Genres" (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE;


ALTER TABLE IF EXISTS public."movieGenres"
    ADD CONSTRAINT "movieId" FOREIGN KEY ("movieId")
    REFERENCES public."moviesData" ("movieId") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE; 


ALTER TABLE IF EXISTS public."ogMoviesData"
    ADD CONSTRAINT "movieId" FOREIGN KEY ("movieId")
    REFERENCES public."moviesData" ("movieId") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE;

END;