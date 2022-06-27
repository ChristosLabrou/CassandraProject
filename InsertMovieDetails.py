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
