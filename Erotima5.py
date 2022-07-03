import csv
import AstraConnect
import GlobalVariables as GV
import time
from cassandra import ConsistencyLevel
import cassandra
from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args
from operator import itemgetter

def InsertTags(profile, truncate):
    session = AstraConnect.AstraConnect(profile)
    session.execute('USE movie_database')
    if(truncate):
        session.execute('TRUNCATE movie_database.movietags')

    movieTags = []
    selectAll = session.prepare('SELECT * FROM TempMovieTags')
    importedList = session.execute(selectAll)

    for row in importedList:
        movieTags.append(row)


    insertTagsQuery = session.prepare('INSERT INTO MovieTags (movieId, avgRating, tags, title) VALUES (?, ?, ?, ?)')

    print("Started Insertion")
    startTime = time.time()
    execute_concurrent_with_args(session, insertTagsQuery, movieTags, 90)
    endTime = time.time()
    file = open("TimeComparison.txt", 'a')
    file.write(AstraConnect.ProfileToString(profile) + " time: %f\n" % (endTime-startTime))
    file.close()