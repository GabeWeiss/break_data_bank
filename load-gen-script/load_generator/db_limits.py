import random

# NOTE: These values are represented as the number of rows inserted
# in the db_clean setup code. If the schema for the db changes, or if
# the number of inserts/complexity in that file changes, these values will
# need to change in order to avoid any issues with overflowing our indexes
# in the database
COLOR_COUNT = 6
SHAPE_COUNT = 13
ARTIST_COUNT = 22

def random_shape():
    return random.randint(1, SHAPE_COUNT)

def random_color():
    return random.randint(1, COLOR_COUNT)

def random_artist():
    return random.randint(1, ARTIST_COUNT)
