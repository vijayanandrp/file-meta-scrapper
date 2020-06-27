from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy, ConsistencyLevel
from cassandra.query import tuple_factory
from logger import Logger

log = Logger(stream_output=False).defaults(name_class='lib_cassandra')


class Cassandra:
    def __init__(self):
        self.profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
            retry_policy=DowngradingConsistencyRetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=15,
            row_factory=tuple_factory)
        self.cluster = Cluster(execution_profiles={
                               EXEC_PROFILE_DEFAULT: self.profile})
        self.session = self.cluster.connect()
        self.key_space = None
        self.table_name = None

    def test_connection(self):
        log.info('Test Connection ')
        log.info(self.session.execute(
            "SELECT release_version FROM system.local").one())
        log.info(self.session.execute(
            "SELECT cluster_name, listen_address FROM system.local").one())

    def connection_details(self):
        """ get the details of keyspace and table name"""
        log.info('KeySpace = ' + self.key_space)
        log.info('Tablename = ' + self.table_name)

    def create_keyspace(self, key_space='master_db'):
        """ create keyspace """
        if key_space and key_space.lower() != self.key_space:
            self.key_space = key_space.lower()

        query = """CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };""" % self.key_space
        log.info(query)
        self.session.execute(query)
        self.use_space()

    def use_space(self):
        # self.session.set_keyspace('users') # or you can do this instead
        query = 'USE {};'.format(self.key_space)
        log.info(query)
        self.session.execute(query)

    def create_table(self, table_name='meta_data'):
        """ create table """
        if table_name and table_name.lower() != self.table_name:
            self.table_name = table_name.lower()
        query = """CREATE TABLE IF NOT EXISTS {} (SourceFile TEXT, artifact_id UUID, source_id TEXT, file_filetype TEXT, PRIMARY KEY ((file_filetype, artifact_id), source_id));""".format(
            self.table_name)
        log.info(query)
        self.session.execute(query)

    def get_columns(self):
        """ get columns from the table """
        query = """SELECT column_name FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}';""".format(
            self.key_space, self.table_name)
        log.info(query)
        cols = self.session.execute(query)
        cols = [col[0] for col in cols]
        log.debug('Column names - {}'.format(cols))
        return cols

    def add_missing_column(self, column):
        """ add the missing column name to the table """

        if 'date' in column:
            data_type = 'TIMESTAMP'
        elif any(x in column for x in ['height', 'width', 'depth']):
            data_type = 'INT'
        else:
            data_type = 'TEXT'

        query = """ALTER TABLE {} ADD {} {};""".format(
            self.table_name, column, data_type)
        log.info(query)
        self.session.execute(query)

    @staticmethod
    def parse_key(key):
        """ remove special character in the key name """
        return key.strip().lower().replace(':', '_')

    @staticmethod
    def adjust_timestamp(timestamp):
        try:
            from time import gmtime, strftime
            time_zone = strftime("%z", gmtime())
            date = timestamp.split(' ')[0].replace(':', '-')
            time = timestamp.split(' ')[1].split('-')[0]

            if any(x in timestamp for x in ['+', 'Z']):
                return ' '.join([date, time])

            timestamp = ' '.join([date, time, time_zone])
            if "0000-00-00" in timestamp:
                return ""
            return timestamp
        except Exception as err:
            log.info('Error - timestamp parsing - {}'.format(err))
            log.info('>>> - ' + timestamp)
            return ""

    def parse_json(self, value):
        """ convert JSON to string and convert ' to " """
        parsed = dict()
        for key in value.keys():
            key1 = '"{}"'.format(self.parse_key(key))
            if any(x in key1 for x in ['height', 'width', 'depth']):
                value1 = '{}'.format(value[key])
            elif 'date' in key1:
                value1 = '"{}"'.format(self.adjust_timestamp(value[key]))
            else:
                value1 = '"{}"'.format(value[key])

            parsed[key1] = value1
        parsed = str(parsed).replace("'", '').replace('\\', '')
        log.debug(parsed)
        return parsed

    def insert_json(self, value):
        """ insert json data to the table """
        keys = value.keys()
        keys = [self.parse_key(key) for key in keys]

        cols = self.get_columns()
        missing_cols = list(set(keys) - set(cols))
        if missing_cols:
            [self.add_missing_column(column=col)
             for col in missing_cols if col]

        value = self.parse_json(value)
        query = """INSERT INTO {} JSON '{}';""".format(self.table_name, value)
        log.info(query)
        self.session.execute(query)

    def drop_keyspace(self):
        query = 'DROP KEYSPACE IF EXISTS {};'.format(self.key_space)
        log.info(query)
        self.session.execute(query)

    def drop_table(self):
        query = 'DROP TABLE IF EXISTS {};'.format(self.table_name)
        log.info(query)
        self.session.execute(query)

    def truncate_table(self):
        query = 'TRUNCATE TABLE {};'.format(self.table_name)
        log.info(query)
        self.session.execute(query)
