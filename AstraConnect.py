def AstraConnect():
    cloud_config = {
    'secure_connect_bundle' : 'secure-connect-cassandraproject.zip'
}
#tokenData #Column 0 is Client ID, Column 1 is Secret, Column 2 is Role, Column 3 is Role
    with open('GeneratedToken', 'r') as tokencsv:
        csvreader = csv.reader(tokencsv, delimiter=',')
        i = -1
        for row in csvreader:
            if i==-1:
                i=0
                continue
            tokenData=row.copy()
    print("Attempting to connect...")
    auth_provider = PlainTextAuthProvider(tokenData[0], tokenData[1])
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    print("Connection established!!\nThis is where the fun begins")
    session.execute('USE movie_database')