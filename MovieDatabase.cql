CREATE KEYSPACE IF NOT EXISTS MovieDatabase WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : '1'};

CREATE TABLE IF NOT EXISTS MovieDatabase.movie(
    movieId int PRIMARY KEY,
    title text,
    genre text,
    #topTags list<text>,
    productionYear int,
    avg_rating double
);

CREATE TABLE IF NOT EXISTS MovieDatabase.movieRatings(
    movieId int PRIMARY KEY,
    title text,
    ratingList list<double>,
    ratingTimestamps list<timestamp>
);