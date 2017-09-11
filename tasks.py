import csv
import glob
import multiprocessing
import os
import records
import sqlalchemy

from invoke import task


DB_ROOT_URL = os.environ.get('cookcountyjail2_DB_URL')
DB_URL = '{0}/{1}'.format(DB_ROOT_URL, 'cookcountyjail2')

COPY_QUERY_TEMPLATE = """\copy inmates_raw (
    Age_At_Booking,
    Bail_Amount,
    Booking_Date,
    Booking_Id,
    Charges,
    Court_Date,
    Court_Location,
    Gender,
    Height,
    Housing_Location,
    Incomplete,
    Inmate_Hash,
    Race,
    Scrape_Date,
    Weight
)
from '{filename}'
with delimiter ',' csv header;"""

FIELDNAMES = [
    'Age_At_Booking',
    'Bail_Amount',
    'Booking_Date',
    'Booking_Id',
    'Charges',
    'Court_Date',
    'Court_Location',
    'Gender',
    'Height',
    'Housing_Location',
    'Incomplete',
    'Inmate_Hash',
    'Race',
    'Scrape_Date',
    'Weight']

try:
    db = records.Database(DB_URL)
except sqlalchemy.exc.OperationalError:
    print('Database does not exist, creating it')
    # create_db()


@task
def create_db(ctx):
    print('create database')
    engine = sqlalchemy.create_engine('{0}/{1}'.format(DB_ROOT_URL, 'postgres'))
    conn = engine.connect()
    conn.execute('commit')
    try:
        conn.execute('create database cookcountyjail2')
    except sqlalchemy.exc.ProgrammingError:
        print('database already exists')
    finally:
        conn.close()


@task
def drop_db(ctx):
    print('drop database')
    engine = sqlalchemy.create_engine('{0}/{1}'.format(DB_ROOT_URL, 'postgres'))
    conn = engine.connect()
    conn.execute('commit')
    try:
        conn.execute('drop database cookcountyjail2')
    except sqlalchemy.exc.ProgrammingError:
        print('database does not exist')
    finally:
        conn.close()


@task
def create_tables(ctx):
    print('create tables')
    db.query_file('queries/create_tables.sql')


@task
def drop_tables(ctx):
    print('drop')
    db.query_file('queries/drop_tables.sql')


@task
def clear_data(ctx):
    print('clear')
    db.query_file('queries/clear.sql')


@task
def fetch(ctx):
    print('fetch')
    ctx.run('aws s3 sync s3://cookcountyjail.il.propublica.org/daily/ data/raw')


@task
def process(ctx, overwrite=False):
    print('process')

    with open('data/processed/inmates.csv', 'w+') as inmates_f:
        inmates_writer = csv.DictWriter(inmates_f, fieldnames=FIELDNAMES)
        inmates_writer.writeheader()

        pool = multiprocessing.Pool(8)
        pool.map(clean_data, [(relative_path, overwrite) for relative_path in glob.glob('data/raw/*.csv')])


def clean_data(params):
    relative_path, overwrite = params  # @TODO there MUST be a better way

    filename = relative_path.split('/').pop()
    clean_path = 'data/processed/{filename}'.format(filename=filename)

    if overwrite or not os.path.isfile(clean_path):
        print('created {0}'.format(clean_path))
        with open(relative_path) as rf, open (clean_path, 'w+') as wf:
            reader = csv.DictReader(rf)
            writer = csv.DictWriter(wf, quoting=csv.QUOTE_ALL, fieldnames=FIELDNAMES)

            # @TODO determine if appending and skip
            writer.writeheader()

            for row in reader:
                scrapedate = filename.split('.')[0]
                row['Scrape_Date'] = scrapedate
                row['Incomplete'] = row.get('Incomplete', 'false')
                writer.writerow(row)
    else:
        print('skipped {0}'.format(clean_path))

@task
def combine(ctx):
    print('combine')
    ctx.run('/bin/bash ./scripts/combine.sh')

@task
def load(ctx):
    print('load')
    query = COPY_QUERY_TEMPLATE.format(filename=os.path.realpath('data/processed/inmates.csv'))
    ctx.run('psql {db_url} -c "{query}"'.format(db_url=DB_URL, query=query))


@task(drop_tables, create_tables, fetch, process, combine, load)
def reset(ctx):
    pass


@task(drop_db, create_db, create_tables, fetch, process, combine, load)
def bootstrap(ctx):
    pass




