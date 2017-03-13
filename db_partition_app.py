#!/usr/bin/python2.7
#
# Interface for the assignement
#
from __future__ import division
import psycopg2

DATABASE_NAME = 'dds_assgn1'

RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
INPUT_FILE_PATH = 'test_data.dat'

# Meta data about partitions
max_rating = 5.0
rrobin_partition_names = []
range_start_end_map = {}
range_partition_name_map = {}
rrobin_meta_data = []


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    # Reading the ratings file data and storing in a list
    rating_file_obj = open(ratingsfilepath)
    rating_lines = rating_file_obj.readlines()
    rating_file_obj.close()

    cur = openconnection.cursor()
    try:
        # Create the ratings table
        cur.execute('''CREATE TABLE IF NOT EXISTS %s (userid INT NOT NULL,
                movieid INT NOT NULL,
                rating FLOAT NOT NULL);''' % (ratingstablename))

        for line in rating_lines:
            rating = line.split('::')
            cur.execute('INSERT INTO %s (userid, movieid, rating) VALUES (%d, %d, %f);' % (
                ratingstablename, int(rating[0]), int(rating[1]), float(rating[2])))

        openconnection.commit()

    except Exception as detail:
        print "OOPS! Error in creating or inserting data in table ==> ", detail


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    num_partitions = int(numberofpartitions)
    range_size = max_rating / num_partitions

    start_of_range = 0.0
    end_of_range = start_of_range + range_size

    cur = openconnection.cursor()

    for part in range(num_partitions):

        partition_name = RANGE_TABLE_PREFIX + str(part)
        range_partition_name_map[str(start_of_range) + '-' + str(end_of_range)] = partition_name
        range_start_end_map[start_of_range] = end_of_range

        try:
            if part < num_partitions - 1:
                cur.execute('CREATE TABLE IF NOT EXISTS %s ('
                            'userid INT NOT NULL,'
                            'movieid INT NOT NULL,'
                            'rating FLOAT NOT NULL CHECK ( rating >= %f AND rating < %f)'
                            ');' % (partition_name, start_of_range, end_of_range))
            else:
                cur.execute('CREATE TABLE IF NOT EXISTS %s ('
                            'userid INT NOT NULL,'
                            'movieid INT NOT NULL,'
                            'rating FLOAT NOT NULL CHECK ( rating >= %f AND rating <= %f)'
                            ');' % (partition_name, start_of_range, end_of_range))


        except Exception as detail:
            print "OOPS! Error in creating partition tables ==> ", detail

        start_of_range += range_size
        end_of_range += range_size

    openconnection.commit()

    cur.execute('SELECT * FROM ' + ratingstablename + ';')
    rows = cur.fetchall()

    for row in rows:

        user_id = row[0]
        movie_id = row[1]
        rating = row[2]

        for start_range in range_start_end_map:
            end_range = range_start_end_map[start_range]

            if (rating >= start_range and rating < end_range) or (rating == max_rating and end_range == max_rating):
                full_range = str(start_range) + '-' + str(end_range)
                part_name = range_partition_name_map[full_range]
                cur.execute('INSERT INTO %s (userid, movieid, rating) VALUES (%d, %d, %f);'
                            % (part_name, int(user_id), int(movie_id), float(rating)))

                break

    openconnection.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    try:
        for start in range_start_end_map:
            stop = range_start_end_map[start]
            if (rating >= start and rating < stop) or (rating == max_rating and stop == max_rating):
                full_range = str(start) + '-' + str(stop)
                part_name = range_partition_name_map[full_range]
                cur.execute('INSERT INTO %s (userid, movieid, rating) VALUES (%d, %d, %f);'
                            % (part_name, int(userid), int(itemid), float(rating)))
                break

    except Exception as detail:
        print "Error occurred in insertion due to ==> ", detail

    openconnection.commit()
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    rrobin_meta_data.append(numberofpartitions)

    for part in range(numberofpartitions):
        partition_name = RROBIN_TABLE_PREFIX + str(part)
        rrobin_partition_names.append(partition_name)

        cur.execute('CREATE TABLE IF NOT EXISTS %s ('
                    'userid INT NOT NULL,'
                    'movieid INT NOT NULL,'
                    'rating FLOAT NOT NULL'
                    ');' % (partition_name))

    openconnection.commit()

    cur.execute('SELECT * FROM ' + ratingstablename + ';')
    rows = cur.fetchall()
    rrobin_index = 0

    for row in rows:

        user_id = row[0]
        movie_id = row[1]
        rating = row[2]

        rrobin_index = rrobin_index % numberofpartitions
        rrobin_part_name = RROBIN_TABLE_PREFIX + str(rrobin_index)

        cur.execute('INSERT INTO %s (userid, movieid, rating) VALUES (%d, %d, %f);'
                    % (rrobin_part_name, int(user_id), int(movie_id), float(rating)))

        rrobin_index += 1

    rrobin_meta_data.append(rrobin_index)
    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()

    rrobin_partition = rrobin_meta_data[0]
    rrobin_global_index = rrobin_meta_data[1]

    rrobin_index = rrobin_global_index % rrobin_partition
    rrobin_part_name = RROBIN_TABLE_PREFIX + str(rrobin_index)

    try:
        cur.execute('INSERT INTO %s (userid, movieid, rating) VALUES (%d, %d, %f);'
                  % (rrobin_part_name, int(userid), int(itemid), float(rating)))

    except Exception as detail:
        print "Error occurred in insertion due to ==> ", detail

    rrobin_global_index += 1
    rrobin_meta_data[1] = rrobin_global_index

    openconnection.commit()
    cur.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()

    try:
        for range in range_partition_name_map:
            range_part_name = range_partition_name_map[str(range)]
            cur.execute('DROP TABLE IF EXISTS %s;' % (range_part_name))

        for rrobin_part_name in rrobin_partition_names:
            cur.execute('DROP TABLE IF EXISTS %s;' % (rrobin_part_name))

    except Exception as detail:
        print 'OOPS! Error occurred while dropping tables ==>', detail

    openconnection.commit()
    cur.close()


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings(RATINGS_TABLE, 'test_data.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
