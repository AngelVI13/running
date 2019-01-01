import sqlite3
import logging


class DbManager:
    """ Manages SQLite database and all data transactions. Database is used for storing and retrieving
        results of HIL testing and contains the following tables:
        - general_info: stores system, run time, general information;
        - hil_tests: contains results of HIL tests. """

    def __init__(self, db_name):
        """ Initializes a connection to the SQLite database, a cursor for the connection and all DB table attributes.
        @param db_name: name of the database file (.sqlite) """

        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

        # General information related variables
        self.general_table = 'general_info'
        self.workout_date = ''
        self.workout_type = ''
        self.workout_duration = ''

    # ----------- DB and table management ------------------------------------------------------------------------------

    @staticmethod
    def _get_general_table_header():
        """ Defines header of the general info table. """

        header = ['workout_date', 'workout_type', 'workout_duration']
        return header

    # ------------------------------------------------------------------------------------------------------------------

    def create_db_table(self, table_name, header_fields):
        """ Creates a table.
        @param table_name: name of a table to create
        @param header_fields: a list of header fields for the table """

        fields = '", "'.join(header_fields)
        sql_statement = 'CREATE TABLE IF NOT EXISTS {0} ("{1}")'.format(table_name, fields)

        self.cursor.execute(sql_statement)
        self.commit_changes()

    def drop_db_table(self, table_name):
        """ Drops a table.
        @param table_name: name of a table to drop """

        sql_statement = 'DROP TABLE IF EXISTS {0}'.format(table_name)

        self.cursor.execute(sql_statement)
        self.commit_changes()

    def create_all_tables(self):
        """ Creates tables for general info and HIL test results. """

        logging.debug('Creating "{gen}" table.'.format(gen=self.general_table))

        self.create_db_table(self.general_table, self._get_general_table_header())

    def drop_all_tables(self):
        """ Drops general info and HIL test results' tables. """

        logging.debug('Dropping "{gen}" table.'.format(gen=self.general_table))

        self.drop_db_table(self.general_table)

    def count_tables(self):
        """ Gets a number of tables created in DB. """

        sql_statement = 'SELECT COUNT(*) FROM sqlite_master WHERE type="table"'

        self.cursor.execute(sql_statement)
        result = self.cursor.fetchone()[0]

        return result

    def table_exists(self, table_name):
        """ Checks if table is created.
        @param table_name: name of a table to look for """

        sql_statement = 'SELECT COUNT(*) FROM sqlite_master ' \
                        'WHERE type="table" and tbl_name="{table}"'.format(table=table_name)

        self.cursor.execute(sql_statement)
        result = self.cursor.fetchone()[0]

        return result

    def count_records(self, table_name):
        """ Gets a number of records in specified table.
        @param table_name: name of a table to get record count from """

        if not self.table_exists(table_name):
            return 0  # TODO if table doesnt exists return error or -1

        sql_statement = 'SELECT COUNT(*) FROM {table}'.format(table=table_name)

        self.cursor.execute(sql_statement)
        result = self.cursor.fetchone()[0]

        return result

    def commit_changes(self):
        """ Commits all changes in the database. """

        self.connection.commit()

    def close_connection(self):
        """ Commits changes and closes database connection. """

        self.commit_changes()
        self.connection.close()

    # ----------- Table records' management ----------------

    def add_gen_rec(self):
        """ Adds a new record with general information to general purpose table. """

        values = '"{date}", "{type}", "{duration}"'.format(
            date=self.workout_date, type=self.workout_type, duration=self.workout_duration)

        sql_statement = 'INSERT INTO {table_name} VALUES ({values})'.format(
            table_name=self.general_table, values=values)

        self.cursor.execute(sql_statement)
        self.commit_changes()

    def get_all_table_recs(self, table_name):
        """ Fetches all records from the specified table. """

        sql_statement = 'SELECT * FROM {0} ORDER BY rowid'.format(table_name)

        self.cursor.execute(sql_statement)
        results = self.cursor.fetchall()

        return results

    def get_one_specific_rec(self, query):
        """ Fetches one record that matches criteria of specified query. """

        self.cursor.execute(query)
        result = self.cursor.fetchone()

        return result

    def get_all_specific_recs(self, query):
        """ Fetches all records that match criteria of specified query. """

        self.cursor.execute(query)
        results = self.cursor.fetchall()

        return results

    def update_db(self, query):
        """ Makes updates in DB according to criteria of specified query. """

        self.cursor.execute(query)

    # ----------- Attribute setters ----------------

    def set_workout_info(self, workout_date, workout_type, workout_duration):
        """Sets workout info.

        @type workout_date: datetime.datetime
        @param workout_date: Datetime of workout.

        @type workout_type: str
        @param workout_type: Type of workout (Easy, Tempo, Interval, etc.).

        @type workout_duration: int
        @param workout_duration: Duration of workout.
        """

        self.workout_date = workout_date
        self.workout_type = workout_type
        self.workout_duration = workout_duration

    # ----------- Values' reset ----------------

    def reset_fields(self):
        """ Restores attributes' initial values. """

        self.workout_date = ''
        self.workout_type = ''
        self.workout_duration = ''
