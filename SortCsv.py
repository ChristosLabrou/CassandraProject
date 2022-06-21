from operator import itemgetter
import csv
import time

def SortCsvFile(inputFile, outputFile, columnToSortBy):
    #Reads from inputFile then sorts the list by given column and saves it to outputFile
    j=-1
    dataList = [] #Column 0 = userId, Column 1 = movieId, Column 2 = (rating|tag), Column 3 = timestamp
    with open(inputFile, 'r') as input:
        print("Started reading csv")
        startTime = time.time()
        csv_reader = csv.reader(input,delimiter=',')
        for row in csv_reader:
            if j==-1:
                j=0
                continue
            row[0]=int(row[0])
            row[1]=int(row[1])
            dataList.append(row)
        print("Finished reading csv, took %s seconds. Started sorting" %(time.time()-startTime))


        startTime = time.time()
    
        dataList.sort(key=itemgetter(columnToSortBy))
        endTime = time.time() - startTime
        print("Finished sorting. Took %s seconds to sort" %endTime)
    with open(outputFile, 'w') as output:
        writer = csv.writer(output)
        print("Started writing")
        startTime = time.time()
        writer.writerows(dataList)
        endTime = time.time() - startTime
        print("Finished. Took %s seconds to write the csv file" %endTime)
#end
