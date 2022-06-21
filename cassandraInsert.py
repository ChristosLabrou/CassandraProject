from cassandra.cluster import Cluster
from operator import itemgetter
import csv
import time
import datetime

#cluster = Cluster('127.0.0.1', port=9042)
#session = cluster.connect('MovieDatabase')
genre = [[]]
movieRows = []
fields = []
productionYear = []
i=-1
with open('movie.csv', 'r') as moviecsv:
    csv_reader = csv.reader(moviecsv, delimiter=',')
    #fields = csv_reader.next()
    for row in csv_reader:
        if i==-1:
            i=0
            continue
        movieRows.append(row)
        movieRows[i][1]=movieRows[i][1][:-6]
        #print(movieRows[i][1])
        productionYear.append(row[1][-5:-1])
        i=i+1

j=-1
ratingslist = []
temp = []
with open('rating.csv', 'r') as ratingcsv:
    csv_reader = csv.reader(ratingcsv,delimiter=',')
    for row in csv_reader:
        if j==-1:
            j=0
            continue
        row[0]=int(row[0])
        row[1]=int(row[1])
        row[2]=int(row[2])
        ratingslist.append(row)
        #j=j+1
    ratingslist.sort(key=itemgetter(1))
    for x in range (0, len(ratingslist)):
        print(ratingslist[x])