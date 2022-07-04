import csv
import AstraConnect
import GlobalVariables as GV
import time
from cassandra.concurrent import execute_concurrent_with_args
import random

def InsertFullDetails(profile, truncate):
    startTime = time.time()
    movies = []
    avgRatings = []
    session = AstraConnect.AstraConnect(profile)
    session.execute('USE movie_database')
    if (truncate):
        session.execute('TRUNCATE moviefulldetails')
    with open(GV.CSVPATH+'fulldetails.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            row[0] = int(row[0])
            row[2] = row[2].split('|')
            try:
                row[3] = int(row[3])
            except:
                row[3] = 0.0
            movies.append(row)
    
    with open(GV.CSVPATH+'avgRating.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            temp = int(row[0])
            row[0] = round(float(row[1]), 2)
            row[1] = temp
            avgRatings.append(row)

    mostCommonTags = []
    with open(GV.CSVPATH+'mostCommonTags.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            temp = int(row[0])
            row.pop(0)
            newList = [row, temp]
            mostCommonTags.append(newList)

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

def SelectFullDetails(profile, titles):
    session = AstraConnect.AstraConnect(profile)
    query = session.prepare("SELECT * FROM movie_database.MovieFullDetails WHERE title=? ALLOW FILTERING")
    startTime = []
    duration = []
    results = []
    for i in range(0, len(titles)):
        startTime.append(time.time())
        results.append(session.execute(query, (titles[i],)))
        duration.append(time.time() - startTime[len(startTime)-1])

    exportedList = []
    for row in results:
        for element in row:
            exportedList.append([element])
    with open(GV.CSVPATH+'erotima2.csv', 'w') as export:
        writer = csv.writer(export)
        writer.writerows(exportedList)
    file = open("Erotima2.txt", "a")
    file.write(AstraConnect.ProfileToString(profile) + " times:\n")
    for line in duration:
        file.write(f"{line}\n")
    file.close()

def RandomizeFullDetailsQueries(number):
    allTitles = []
    with open(GV.CSVPATH+'fulldetails.csv', 'r') as input:
        csv_reader = csv.reader(input, delimiter=',')
        for row in csv_reader:
            allTitles.append(row[1])
    titles = random.sample(population=allTitles, k=number)
    return titles

