import csv
import AstraConnect
import GlobalVariables as GV
import time
from cassandra.concurrent import execute_concurrent_with_args

def InsertGenreByYear(profile, truncate):
    startTime = time.time()
    session = AstraConnect.AstraConnect(profile)
    session.execute('USE movie_database')
    if (truncate):
        session.execute('TRUNCATE movie_database.MovieGenreByYear')
    
    movies = []

    with open(GV.CSVPATH+'fulldetails.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            row[0] = int(row[0])
            row[2] = row[2].split('|')
            try:
                row[3] = int(row[3])
            except:
                row[3] = 0
            movies.append(row)
    
    insertQuery = session.prepare('INSERT INTO MovieGenreByYear (movieId, title, genre, productionYear) VALUES(?, ?, ?, ?)')
    execute_concurrent_with_args(session, insertQuery, movies, 90)
    endTime = time.time()
    file = open("TimeComparison.txt", 'a')
    file.write(AstraConnect.ProfileToString(profile) + " time: %f\n" % (endTime-startTime))
    file.close()