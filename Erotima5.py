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
    startTime = time.time()
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

    execute_concurrent_with_args(session, insertTagsQuery, movieTags, 90)
    endTime = time.time()
    file = open("TimeComparison.txt", 'a')
    file.write(AstraConnect.ProfileToString(profile) + " time: %f\n" % (endTime-startTime))
    file.close()

#This function was used once. TempMovieTags was a placeholder to help me insert data in MovieTags table with proper keys
def InsertTempTags(profile, truncate):
    movieTags = []
    session = AstraConnect.AstraConnect(profile)
    session.execute('USE movie_database')
    if(truncate):
        session.execute('TRUNCATE movie_database.tempmovietags')

    selectFullDetails = session.prepare('SELECT * FROM MovieFullDetails')
    importedList = session.execute(selectFullDetails)
    #Colum 0 is ID, Column 1 is avgRating, Column 2 is Genres, Column 3 is most common tags, Column 4 Year, Column 5 title
    detailsList = []
    for row in importedList:
        if(row[1] is None):
            tempList = [int(row[0]), 0.0, row[5]]
        else:
            tempList = [int(row[0]), float(row[1]), row[5]]
        detailsList.append(tempList) #Colum 0 is ID, Column 1 is avgRating, Column 2 is title
    del importedList

    IDs = []
    tags = []
    tagsListToBeInserted = []

    with open(GV.CSVPATH+'movieAllTags.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            #separator = ' | '
            IDs.append(int(row[0]))
            row.pop(0)
            #separator = separator.join(row)
            tags.append(row)
            tagsListToBeInserted.append([tags[len(tags)-1], IDs[len(IDs)-1]])
    
    insertPrimaryKey = session.prepare('INSERT INTO TempMovieTags (movieId, avgRating, title) VALUES (?, ?, ?)')
    updateTags = session.prepare('UPDATE TempMovieTags SET tags=? WHERE movieId=?')

    execute_concurrent_with_args(session, insertPrimaryKey, detailsList, 90)
    execute_concurrent_with_args(session, updateTags, tagsListToBeInserted, 90)