CREATE KEYSPACE IF NOT EXISTS movie_database WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : '1'};

CREATE TABLE IF NOT EXISTS movie_database.MovieFullDetails(
    movieId int PRIMARY KEY,
    title text,
    genre text,
    productionYear int,
    avgRating double,
    mostCommonTags text
);

CREATE TABLE IF NOT EXISTS movie_database.MovieRatings(
    movieId int PRIMARY KEY,
    title text,
    ratingList list<double>,
    ratingTimestamps list<timestamp>
);

CREATE TABLE IF NOT EXISTS movie_database.MovieTags(
    movieId int PRIMARY KEY,
    title text,
    avgRating double,
    tags list<text>
);