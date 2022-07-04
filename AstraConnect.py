from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
import csv
import GlobalVariables as GV

def AstraConnect(profile):
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
    cluster = Cluster(auth_provider=auth_provider, cloud=cloud_config, connect_timeout=10000, execution_profiles={EXEC_PROFILE_DEFAULT:profile})
    session = cluster.connect()
    print("Connection established!!\nThis is where the fun begins")
    return session

def ProfileToString(profile):
    reply = ''
    if (profile == QuorumProfile):
        reply = 'Quorum'
    elif (profile == AllProfile):
        reply = 'All'
    elif (profile == TwoProfile):
        reply = 'Two'
    elif (profile == OneProfile):
        reply = 'One'
    return reply


QuorumProfile = ExecutionProfile(
    retry_policy=DowngradingConsistencyRetryPolicy(),
    consistency_level=ConsistencyLevel.QUORUM,
    serial_consistency_level=ConsistencyLevel.SERIAL,
    request_timeout=15,
    row_factory=tuple_factory)

AllProfile = ExecutionProfile(
    retry_policy=DowngradingConsistencyRetryPolicy(),
    consistency_level=ConsistencyLevel.ALL,
    serial_consistency_level=ConsistencyLevel.SERIAL,
    request_timeout=15,
    row_factory=tuple_factory)

TwoProfile = ExecutionProfile(
    retry_policy=DowngradingConsistencyRetryPolicy(),
    consistency_level=ConsistencyLevel.TWO,
    serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    request_timeout=15,
    row_factory=tuple_factory)

OneProfile = ExecutionProfile(
    retry_policy=DowngradingConsistencyRetryPolicy(),
    consistency_level=ConsistencyLevel.ONE,
    serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    request_timeout=15,
    row_factory=tuple_factory)