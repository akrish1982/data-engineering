try:
    from cassandra.cluster import Cluster
    from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
    from cassandra.auth import PlainTextAuthProvider
    from cassandra import ConsistencyLevel
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from KeyspacesRetryPolicy import KeyspacesRetryPolicy

except ImportError:
    raise RuntimeError('Required packages Failed To install please run "python Setup.py install" command or install '
                    'using pip')
class CassandraAccess:
    def __init__(self):
        pass

    def pd_to_cassandra(self,df):
        pass
        ssl_context = SSLContext(PROTOCOL_TLSv1_2 )
        ssl_context.load_verify_locations('data-pipelines/sf-class2-root.crt')
        ssl_context.verify_mode = CERT_REQUIRED
        auth_provider = PlainTextAuthProvider(username='data-egineering-at-163926246741', password='jgjevjCy26Ue1xrJ5ctdJlIUT95ltVPgbcHVVVQHme0=')
        profile = ExecutionProfile(
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            retry_policy=KeyspacesRetryPolicy(RETRY_MAX_ATTEMPTS=5))
        cluster = Cluster(['cassandra.us-east-1.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142,
                          execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        session = cluster.connect()
        query = 'INSERT INTO stock_information.stats_valuation_recent("Ticker","Attribute","Recent") VALUES (?,?,?)'
        prepared = session.prepare(query)
        for index, row  in df.iterrows(): #NOT THE BEST PERFORMANCE<NEEDS TO BE REPLACED>
            session.execute(prepared, (row['Ticker'], row['Attribute'],row['Recent']))

        # # r = session.execute('SELECT * FROM stock_information.stats_valuation_recent')
        # print(r.current_rows)



