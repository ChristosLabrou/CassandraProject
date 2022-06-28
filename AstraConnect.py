from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import GlobalVariables as GV

def AstraConnect():
    cloud_config = {'secure_connect_bundle' : GV.MAINPATH+'secure-connect-cassandraproject.zip'}
    #tokenData #Column 0 is Client ID, Column 1 is Secret, Column 2 is Role, Column 3 is Role
    with open(GV.TOKENS+'GeneratedTokenAdmin', 'r') as tokencsv:
        csvreader = csv.reader(tokencsv, delimiter=',')
        i = -1
        for row in csvreader:
            if i==-1:
                i=0
                continue
            tokenData=row.copy()
    print("Attempting to connect...")
    auth_provider = PlainTextAuthProvider(tokenData[0], tokenData[1])
    cluster = Cluster(auth_provider=auth_provider, cloud=cloud_config, connect_timeout=10000)
    session = cluster.connect()
    print("Connection established!!\nThis is where the fun begins")
    session.execute('USE movie_database')
    return session