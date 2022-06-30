import csv
import AstraConnect
import GlobalVariables as GV
import time
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args

def InsertFullDetails():
    movies = []

    session = AstraConnect.AstraConnect()
    with open(GV.CSVPATH+'fulldetails.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            movies.append(row)
        for i in range(0, len(movies)):
            movies[i][0] = int(movies[i][0])
            try:
                movies[i][3] = int(movies[i][3])
            except:
                movies[i][3] = None
    insertQuery = session.prepare('INSERT INTO movie_database.moviefulldetails (movieId, title, genre, productionYear) VALUES (?, ?, ?, ?)')
    print("Starting insertion")
    startTime = time.time()
    execute_concurrent_with_args(session, insertQuery, movies, 90)

    updateQuery = session.prepare('UPDATE movie_databse.moviefulldetails SET avgRating=? WHERE movieId=?')
    execute_concurrent_with_args(session, updateQuery)

    print("Quorum took %f seconds to finish" % time.time() - startTime())
InsertFullDetails()