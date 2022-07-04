import csv
import AstraConnect
import GlobalVariables as GV
import time
from cassandra.concurrent import execute_concurrent_with_args
from operator import itemgetter

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

def SelectGenreByYear(profile, genres):
    session = AstraConnect.AstraConnect(profile)
    query = session.prepare("SELECT title, productionYear FROM movie_database.MovieGenreByYear WHERE genre CONTAINS ? ALLOW FILTERING")
    startTime = []
    duration = []
    results = []
    startTime =time.time()
    results.append(session.execute(query, (genres[0],)))

    exportedList = []
    for row in results:
        for element in row:
            exportedList.append([element[0], element[1]])
    exportedList.sort(key=itemgetter(1), reverse=True)
    file = open("Erotima3output.txt", "a")
    for i in range(0, 5):
        file.write(exportedList[i][0]+"\n")
    file.close()
    duration = time.time() - startTime
    file = open("Erotima3.txt", "a")
    file.write(f"{duration}\n")
    file.close()
    
SelectGenreByYear(AstraConnect.OneProfile, ["Adventure"])