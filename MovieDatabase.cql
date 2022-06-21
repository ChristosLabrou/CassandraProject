CREATE KEYSPACE IF NOT EXISTS MovieDatabase WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : '1'};

CREATE TABLE IF NOT EXISTS MovieDatabase.MovieFullDetails(
    movieId int PRIMARY KEY,
    title text,
    genre text,
    #topTags list<text>,
    productionYear int,
    avg_rating double
);

CREATE TABLE IF NOT EXISTS MovieDatabase.MovieRatings(
    movieId int PRIMARY KEY,
    title text,
    ratingList list<double>,
    ratingTimestamps list<timestamp>
);

CREATE TABLE IF NOT EXISTS MovieDatabase.MovieTags(
    movieId int PRIMARY KEY,
    title text,
    avg_rating double,
    tags list<text>
)