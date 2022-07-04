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

def SelectTitle(profile, word):
    session = AstraConnect.AstraConnect(profile)
    query = session.prepare("SELECT title FROM movie_database.MovieTitle")
    startTime = time.time()
    duration = []
    results = []

    results.append(session.execute(query))
    exportedList = []
    for row in results:
        for element in row:
            exportedList.append(element[0])
    exportedList.sort()
    titlesList = []
    for i in range(0, len(exportedList)):
        if (word in exportedList[i]):
            titlesList.append(exportedList[i])

    #file = open("Erotima4output.txt", "a")
    #for i in range(0, 5):
        #file.write(titlesList[i]+"\n")
    #file.close()
    duration = time.time() - startTime
    file = open('Erotima4.txt', 'a')
    file.write(f"{duration}\n")
    file.close()