from cassandra.cluster import Cluster
import csv

#cluster = Cluster('127.0.0.1', port=9042)
#session = cluster.connect('MovieDatabase')
genre = [[]]
movieRows = []
fields = []
with open('movie.csv', 'r') as moviecsv:
    csv_reader = csv.reader(moviecsv, delimiter=',')
    fields = csv_reader.next()
    for row in csv_reader:
        movieRows.append(row)