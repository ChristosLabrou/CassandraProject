import csv
import time
from sqlalchemy import false, null, true
import AstraConnect

#session = AstraConnect.AstraConnect()
moviesList = [] #Column 0 is movieId, Column 1 is title, Column 2 is genres
productionYear = []
taintedTitles = [] #This list saves all the movies (via id) which do not have production year in their title
i=-1
currentMovieIsSafe = true
print("Reading movies csv.")
startTime = time.time()
with open('movie.csv', 'r') as moviecsv:
    csv_reader = csv.reader(moviecsv, delimiter=',')
    for row in csv_reader:
        #I want to skip first row because it contains column names. This is a stupid way to do it but it works
        if i==-1:
            i=0
            continue
        moviesList.append(row)
        row[1] = row[1].rstrip() #Trim whitespace
        #Note: We can't trim parenthesis yet because it messes with title/production year
        productionYear.append(row[1].rsplit('(')[1])
        try:
            #if the movie contais year in its name then set that number as year and remove it from title
            productionYear[i] = int(productionYear[i].rstrip(' -()'))
            size = len(moviesList[i][1])
            moviesList[i][1] = moviesList[i][1][:size-6]
        except:
            #otherwise set year to null
            productionYear[i] = null
        moviesList[i][1] = moviesList[i][1].rstrip()
        i=i+1
print("Reading finished. Took %.2f seconds" %(round(time.time()-startTime,2)))
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


startTime = time.time()
print("Start writing txt")

insert2 = open('queries2.cql', 'w')
for i in range(20000,len(moviesList)):
    if(int(moviesList[i][0])<=10543):
        continue
    insert2.write(
        """
        INSERT INTO movie_database.MovieFullDetails(movieId, title, genre, productionYear, avgRating)
        VALUES (%s, $$%s$$, $$%s$$, %s, null);
        """ % (moviesList[i][0],moviesList[i][1], moviesList[i][2], productionYear[i]))

insertQueries = open('queries.cql', 'w')
for i in range(0,len(moviesList)):
    insertQueries.write(
        """
        INSERT INTO movie_database.MovieFullDetails(movieId, title, genre, productionYear, avgRating)
        VALUES (%s, $$%s$$, $$%s$$, %s, null);
        """ % (moviesList[i][0],moviesList[i][1], moviesList[i][2], productionYear[i]))
insertQueries.close()

updateQueries = open('update.cql', 'w')
for i in range(0,len(movieAvgRating)):
    updateQueries.write(
        """
        UPDATE movie_database.MovieFullDetails
        SET avgRating = %s
        WHERE movieId = %s;
        """ % (movieAvgRating[i][1], movieAvgRating[i][0]))
updateQueries.close()
print("Finished writing .txt. Took %.2f" %(time.time()-startTime))

comment = """
for i in range(0, len(moviesList)):
    while (moviesList[i][0]>movieAvgRating[j][0]):
        j=j+1

    if (moviesList[i][0]==movieAvgRating[j][0]):
        session.execute(
        \"\"\"INSERT INTO movie_database.MovieFullDetails(movieId, title, genre, productionYear, avgRating)
        VALUES (%s, %s, %s, %s, %s)
        \"\"\", (moviesList[i][0],moviesList[i][1], moviesList[i][2], productionYear[i][1], movieAvgRating[j][1])
        )
    else:
        session.execute(
        \"\"\"INSERT INTO movie_database.MovieFullDetails(movieId, title, genre, productionYear, avgRating)
        VALUES (%s, %s, %s, %s, %s)
        \"\"\", (moviesList[i][0],moviesList[i][1], moviesList[i][2], productionYear[i][1], null)
        )
"""

