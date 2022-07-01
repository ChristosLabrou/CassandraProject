import csv
import time
import GlobalVariables as GV
import collections

moviesList = [] #Column 0 is movieId, Column 1 is title, Column 2 is genres
productionYear = []
i=-1
print("Reading movies csv.")
startTime = time.time()
with open(GV.CSVPATH+'movie.csv', 'r') as moviecsv:
    csv_reader = csv.reader(moviecsv, delimiter=',')
    for row in csv_reader:
        #I want to skip first row because it contains column names. This is a stupid way to do it but it works
        if i==-1:
            i=0
            continue
        moviesList.append(row)
        row[1] = row[1].rstrip() #Trim whitespace
        #Note: We can't trim parenthesis yet because it messes with title/production year
        if (row[1].find("(")!=-1):
            productionYear.append(row[1].rsplit('(')[len(row[1].rsplit('('))-1])
            #this black magik is needed because some titles contain parenthesis for alternative names
            #eg Seven (aka Se7en) (1995)
        else:
            productionYear.append(None)
        try:
            #if the movie contains the year in its name then set that number as year and remove it from title
            productionYear[i] = int(productionYear[i].rstrip(' -()'))
            size = len(moviesList[i][1])
            moviesList[i][1] = moviesList[i][1][:size-6]
        except:
            #otherwise set year to null
            productionYear[i] = None
        moviesList[i][1] = moviesList[i][1].rstrip()
        i=i+1
print("Reading finished. Took %.2f seconds" %(time.time()-startTime))

j=-1
ratingslist = [] #Column 0 = userId, Column 1 = movieId, Column 2 = rating, Column 3 = timestamp
print("Reading sorted csv")
startTime = time.time()
with open(GV.CSVPATH+'sorted_rating.csv', 'r') as sortedcsv:
    csv_reader = csv.reader(sortedcsv,delimiter=',')
    for row in csv_reader:
        if j==-1:
            j=0
            continue
        ratingslist.append(row)
print("Finished reading. Took %.2f seconds" %(time.time()-startTime))

print("Reading sorted tags")
startTime = time.time()
totalTags = [] #Column 0 is movieId, Column 1 is tag
with open(GV.CSVPATH+'sorted_tag.csv', 'r') as sortedcsv:
    csv_reader = csv.reader(sortedcsv,delimiter=',')
    for row in csv_reader:
        totalTags.append([row[1], row[2]])
print("Finished reading tags. Took %.2f seconds" %(time.time()-startTime))
#-------------------------------
#Separate tags by movie
#-------------------------------

currentMovieTags = [] #Holds all the tags of current movie
tempList = []
tempId = totalTags[0][0]
mostCommonTags = []
mostCommonTagsIDs = []

for i in range(0, len(totalTags)):
    if (tempId==totalTags[i][0]):
        currentMovieTags.append(totalTags[i][1])
    else:
        counter = collections.Counter(currentMovieTags).most_common(5)
        #counter is a list of tuples. Here we break the tuples to keep most common 5 tags only
        for j in range(0, len(counter)):
            tempList.append(counter[j][0])
        mostCommonTags.append(tempList[:])
        mostCommonTagsIDs.append(tempId)
        tempId = totalTags[i][0]
        tempList.clear()
        currentMovieTags.clear()

mergedList = [] #Combines ID & tags. Column 0 is movieId, Columns 1-5 are the 5 most common tags

for i in range(0, len(mostCommonTags)):
    mergedList.append([mostCommonTagsIDs[i]])
    mergedList[i] = mergedList[i] + mostCommonTags[i]

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

with open(GV.CSVPATH+'fulldetails.csv', 'w') as export:
    writer = csv.writer(export)
    for i in range(0, len(moviesList)):
        writer.writerow([moviesList[i][0], moviesList[i][1], moviesList[i][2], productionYear[i]])

with open (GV.CSVPATH+'avgRating.csv', 'w') as export:
    writer = csv.writer(export)
    for i in range(0, len(movieAvgRating)):
        writer.writerow([movieAvgRating[i][0], movieAvgRating[i][1]])

with open(GV.CSVPATH+'mostCommonTags.csv', 'w') as export:
    writer = csv.writer(export)
    writer.writerows(mergedList)