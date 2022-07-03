import csv
import AstraConnect
import GlobalVariables as GV
import time
from cassandra import ConsistencyLevel
import cassandra
from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args

def InsertFullDetails(profile, truncate):
    movies = []
    avgRatings = []
    session = AstraConnect.AstraConnect(profile)
    session.execute('USE movie_database')
    if (truncate):
        session.execute('TRUNCATE moviefulldetails')
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

    mostCommonTags = []
    IDs = []
    with open(GV.CSVPATH+'mostCommonTags.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            temp = int(row[0])
            row.pop(0)
            #row.append(temp)
            #mostCommonTags.append(row)
            #IDs.append(temp)
            separator = '|'
            separator = separator.join(row)
            newList = [separator, temp]
            mostCommonTags.append(newList)


    
    print("Starting insertion")
    startTime = time.time()

    insertQuery = session.prepare('INSERT INTO moviefulldetails (movieId, title, genre, productionYear) VALUES (?, ?, ?, ?)')
    updateAvgRating = session.prepare('UPDATE moviefulldetails SET avgRating=? WHERE movieId=?')
    updateTags = session.prepare('UPDATE moviefulldetails SET mostCommonTags=? WHERE movieId=?')
    
    execute_concurrent_with_args(session, insertQuery, movies, 90) 
    execute_concurrent_with_args(session, updateTags, mostCommonTags, 90)  
    execute_concurrent_with_args(session, updateAvgRating, avgRatings, 90)
    
    endTime = time.time()
    file = open("TimeComparison.txt", 'a')
    file.write(AstraConnect.ProfileToString(profile) + " time: %f\n" % (endTime-startTime))
    file.close()
    