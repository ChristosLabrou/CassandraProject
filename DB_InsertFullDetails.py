import csv
import AstraConnect
import GlobalVariables as GV
import time
from cassandra import ConsistencyLevel
import cassandra
from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args

def InsertFullDetails():
    movies = []
    avgRatings = []
    session = AstraConnect.AstraConnect()
    session.execute('USE movie_database')
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
    
    with open(GV.CSVPATH+'avgRating.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            temp = int(row[0])
            row[0] = round(float(row[1]), 2)
            row[1] = temp
            avgRatings.append(row)

    insertQuery = session.prepare('INSERT INTO moviefulldetails (movieId, title, genre, productionYear) VALUES (?, ?, ?, ?)')
    print("Starting insertion")
    startTime = time.time()
    execute_concurrent_with_args(session, insertQuery, movies, 90)

    updateQuery = session.prepare('UPDATE moviefulldetails SET avgRating=? WHERE movieId=?')
    execute_concurrent_with_args(session, updateQuery, avgRatings, 90)
    endTime = time.time()
    #print("Quorum took %.2f seconds to finish" % (endTime - startTime))
    file = open("TimeComparison.txt", 'a')
    file.write("One time: %f\n" % (endTime-startTime))
    file.close()
InsertFullDetails()