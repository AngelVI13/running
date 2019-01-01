from operator import itemgetter
from itertools import groupby
from db_manager import DbManager


db_name = 'test.sqlite'

dbManager = DbManager(db_name)
# TODO there wont be a need to clean tables because we will be appending to them
# clean up database
dbManager.drop_all_tables()
dbManager.create_all_tables()


def __add_to_database(workout_date, workout_type, workout_duration):
    """ Gathers workout information and puts it into the database. """

    dbManager.set_workout_info(workout_date, workout_type, workout_duration)

    dbManager.add_gen_rec()
    dbManager.reset_fields()


workouts = [
    {'type': 'Easy', 'date': '2018-12-01', 'duration': 30},
    {'type': 'Interval', 'date': '2018-12-03', 'duration': 31},
    {'type': 'Tempo', 'date': '2018-12-02', 'duration': 32},
    {'type': 'Easy', 'date': '2018-12-03', 'duration': 33},
    {'type': 'Interval', 'date': '2018-12-01', 'duration': 34},
    {'type': 'Easy', 'date': '2018-12-04', 'duration': 35},
]


# sort dictionary based on date
workouts.sort(key=itemgetter('date'))

for date, items in groupby(workouts, key=itemgetter('date')):
    print(date)
    for i in items:
        print(' ', i)
        __add_to_database(i['date'], i['type'], i['duration'])


dbManager.close_connection()
print('DB filename:', db_name)