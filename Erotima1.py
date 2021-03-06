import csv
import AstraConnect
import GlobalVariables as GV
import time
import datetime
from cassandra.concurrent import execute_concurrent_with_args

RATING_LIMIT = 10000

def InsertRatings(profile, truncate):
    startTime = time.time()
    session = AstraConnect.AstraConnect(profile)
    session.execute('USE movie_database')
    if (truncate):
        session.execute('TRUNCATE MovieRatings')
    
    ratings = []
    i=-1
    insertQuery = session.prepare('INSERT INTO MovieRatings (userId, movieId, rating, ratingTimestamp) VALUES (?, ?, ?, ?)')
    with open(GV.CSVPATH+'rating.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            if i ==-1:
                i = 0
                continue
            row[0] = int(row[0])
            row[1] = int(row[1])
            row[2] = round(float(row[2]), 2)
            temp = row[3].split(' ') #Remove the exact hour as it isn't required by the query. We only keep the exact day
            row[3] = time.mktime(datetime.datetime.strptime(temp[0], "%Y-%m-%d").timetuple())
            ratings.append(row)
            i = i+1
            if i==RATING_LIMIT:
                execute_concurrent_with_args(session, insertQuery, ratings, 90)
                i = 0
                ratings.clear()
    del ratings
    del insertQuery
    titles = []
    with open(GV.CSVPATH+'fulldetails.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            titles.append([row[1], int(row[0])])
    
    updateQuery = session.prepare('UPDATE MovieRatings SET title=? WHERE movieId=?')
    execute_concurrent_with_args(session, updateQuery, titles, 90)
    endTime = time.time()
    file = open("TimeComparison.txt", 'a')
    file.write(AstraConnect.ProfileToString(profile) + " time: %f\n" % (endTime-startTime))
    file.close()