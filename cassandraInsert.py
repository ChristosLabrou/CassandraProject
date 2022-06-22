from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import time
from sqlalchemy import null
import AstraConnect

AstraConnect()
moviesList = [] #Column 0 is movieId, Column 1 is title, Column 2 is genres
productionYear = []
i=-1
print("Reading movies csv.")
startTime = time.time()
with open('movie.csv', 'r') as moviecsv:
    csv_reader = csv.reader(moviecsv, delimiter=',')
    for row in csv_reader:
        if i==-1:
            i=0
            continue
        moviesList.append(row)
        moviesList[i][1]=moviesList[i][1][:-6]
        productionYear.append(row[1][-5:-1])
        i=i+1
print("Reading finished. Took %f seconds" %(round(time.time()-startTime,2)))
j=-1
ratingslist = [] #Column 0 = userId, Column 1 = movieId, Column 2 = rating, Column 3 = timestamp
print("Reading sorted csv")
startTime = time.time()
with open('sorted_rating.csv', 'r') as sortedcsv:
    csv_reader = csv.reader(sortedcsv,delimiter=',')
    for row in csv_reader:
        if j==-1:
            j=0
            continue
        ratingslist.append(row)
print("Finished reading. Took ", (time.time()-startTime), " seconds")

#-------------------------------
#Calculate movie average rating
#-------------------------------
movieAvgRating = [] #Column 0 is movieId, Column 1 is average rating
sum = float(ratingslist[0][2])
count = 1
tempMovieId = ratingslist[0][1]
for x in range(1, len(ratingslist)):
    if (tempMovieId==ratingslist[x][1]):
        sum = sum + float(ratingslist[x][2])
        count = count+1
    else:
        movieAvgRating.append([ratingslist[x-1][1],round(sum/count,2)])
        sum = float(ratingslist[x][2])
        count = 1
        tempMovieId = ratingslist[x][1]

j=0
for i in range(0, len(moviesList)):
    while (moviesList[i][0]>movieAvgRating[j][0]):
        j=j+1

    if (moviesList[i][0]==movieAvgRating[j][0]):
        session.execute(
        """INSERT INTO movie_database.MovieFullDetails(movieId, title, genre, productionYear, avgRating)
        VALUES (%s, %s, %s, %s, %s)
        """, (moviesList[i][0],moviesList[i][1], moviesList[i][2], productionYear[i], movieAvgRating[j][1])
        )
    else:
        session.execute(
        """INSERT INTO movie_database.MovieFullDetails(movieId, title, genre, productionYear, avgRating)
        VALUES (%s, %s, %s, %s, %s)
        """, (moviesList[i][0],moviesList[i][1], moviesList[i][2], productionYear[i][1], null)
        )
print("Queries successfully executed.\nAnother happy landing.")