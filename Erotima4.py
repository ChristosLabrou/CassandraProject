import csv
import AstraConnect
import GlobalVariables as GV
import time
from cassandra.concurrent import execute_concurrent_with_args

def InsertTitle(profile, truncate):
    startTime = time.time()
    session = AstraConnect.AstraConnect(profile)
    if (truncate):
        session.execute('TRUNCATE movie_database.MovieTitle')
    movieTitles = []
    with open(GV.CSVPATH+'fulldetails.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            tempList = [int(row[0]), row[1]]
            movieTitles.append(tempList)
    
    insertTitles = session.prepare('INSERT INTO movie_database.MovieTitle (movieId, title) VALUES (?, ?)')

    execute_concurrent_with_args(session, insertTitles, movieTitles, 90)
    endTime = time.time()
    file = open("TimeComparison.txt", 'a')
    file.write(AstraConnect.ProfileToString(profile) + " time: %f\n" % (endTime-startTime))
    file.close()