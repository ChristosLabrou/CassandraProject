CREATE KEYSPACE IF NOT EXISTS movie_database WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : '1'};

--Erotima 1
CREATE TABLE IF NOT EXISTS movie_database.MovieRatings(
    userId int,
    movieId int,
    title text,
    rating double,
    ratingTimestamp timestamp,
    PRIMARY KEY ((movieId), ratingTimestamp, userId)
)WITH CLUSTERING ORDER BY (ratingTimestamp ASC, userId ASC);

--Erotima 2
CREATE TABLE IF NOT EXISTS movie_database.MovieFullDetails(
    movieId int,
    title text,
    genre list<text>,
    productionYear int,
    avgRating double,
    mostCommonTags list<text>,
    PRIMARY KEY (movieId)
);

--Erotima 3
CREATE TABLE IF NOT EXISTS movie_database.MovieGenreByYear(
    movieId int,
    title text,
    genre list<text>,
    productionYear int,
    PRIMARY KEY ((productionYear), title)
);

--Erotima 4
CREATE TABLE IF NOT EXISTS movie_database.MovieTitle(
    movieId int,
    title text,
    PRIMARY KEY(movieId, title)
)WITH CLUSTERING ORDER BY (title ASC);

--Erotima 5
CREATE TABLE IF NOT EXISTS movie_database.MovieTags(
    movieId int,
    title text,
    avgRating double,
    tags list<text>,
    PRIMARY KEY(movieId, avgRating)
)WITH CLUSTERING ORDER BY (avgRating DESC);

CREATE TABLE IF NOT EXISTS movie_database.TempMovieTags(
    movieId int,
    title text,
    avgRating double,
    tags list<text>,
    PRIMARY KEY(movieId)
);