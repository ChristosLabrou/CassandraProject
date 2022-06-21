CREATE KEYSPACE IF NOT EXISTS MovieDatabase WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : '1'};

CREATE TABLE IF NOT EXISTS MovieDatabase.movie(
    movieId int PRIMARY KEY,
    title text,
    genre text,
    tag list<text>,
    productionYear int,
    avg_rating double,
    ratingsList list<double>,
    ratingTimestamp list<timestamp>
);
