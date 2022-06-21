from operator import itemgetter
import csv
import time
import datetime

j=-1
ratingslist = [] #Column 0 = userId, Column 1 = movieId, Column 2 = rating, Column 3 = timestamp
with open('rating.csv', 'r') as ratingcsv:
    print("Start reading csv")
    startTime = time.time()
    csv_reader = csv.reader(ratingcsv,delimiter=',')
    for row in csv_reader:
        if j==-1:
            j=0
            continue
        row[0]=int(row[0])
        row[1]=int(row[1])
        ratingslist.append(row)
    print("Finished reading csv, took %s seconds. Started sorting" %(time.time()-startTime))


    startTime = time.time()
    
    #del(csv_reader)
    ratingslist.sort(key=itemgetter(1))
    endTime = time.time() - startTime
    print("Finished sorting. Took %s seconds to sort" %endTime)
with open('sorted_rating.csv', 'w') as sortedCsv:
    writer = csv.writer(sortedCsv)
    print("Started writing")
    startTime = time.time()
    writer.writerows(ratingslist)
    endTime = time.time() - startTime
    print("Finished. Took %s seconds to write the csv file" %endTime)