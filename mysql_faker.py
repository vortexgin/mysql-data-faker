import os
import sys
import getopt
import time
import yaml
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from faker import Faker
from slugify import slugify

with open('mysql.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# from config import SQL instance connection info, and 
# our database information to connect to the db
SQL_HOST = config['connection']['host'] if config['connection']['host'] is not None else os.environ.get("SQL_HOST", "127.0.0.1")
DB_PORT  = config['connection']['port'] if int(config['connection']['port']) is not None else os.environ.get("DB_PORT", 3306)
DB_USER  = config['connection']['user'] if config['connection']['user'] is not None else os.environ.get("DB_USER", "root")
DB_PASS  = config['connection']['password'] if config['connection']['password'] is not None else os.environ.get("DB_PASS", None)
DB_NAME  = config['connection']['dbname'] if config['connection']['dbname'] is not None else os.environ.get("DB_NAME", "test_data_faker")

# Make sure that we have all the pieces we must have in order to connect to our db properly
if not SQL_HOST:
    print ("You have to specify a database host either by environment variable or sets connection on mysql.yaml")
    sys.exit(2)
if not DB_PORT:
    print ("You have to specify a database port either by environment variable or sets connection on mysql.yaml")
    sys.exit(2)
if not DB_USER:
    print ("You have to specify a database user either by environment variable or sets connection on mysql.yaml.")
    sys.exit(2)
if not DB_PASS:
    print ("You have to specify a database password either by environment variable or sets connection on mysql.yaml")
    sys.exit(2)
if not DB_NAME:
    print ("You have to specify a database name either by environment variable or sets connection on mysql.yaml")
    sys.exit(2)

# Wait for our database connection
mydb = None
attempt_num = 0
wait_amount = 1
# backoff_count is the static count for how many times we should try at one
# second increments before expanding the backoff time exponentially
# Once the wait time passes a minute, we'll give up and exit with an error
backoff_count = 5
def connect_database():
    global attempt_num
    global wait_amount
    global mydb
    try:
        mydb = mysql.connector.connect(
            host=SQL_HOST,
            user=DB_USER,
            passwd=DB_PASS,
            port=DB_PORT
        )
    except Error as e:
        attempt_num = attempt_num + 1
        if attempt_num >= backoff_count:
            wait_amount = wait_amount * 2
        print ("Couldn't connect to the MySQL instance, trying again in {} second(s).".format(wait_amount))
        print (e)
        time.sleep(wait_amount)
        if wait_amount > 60:
            print ("Giving up on connecting to the database")
            sys.exit(2)

while mydb == None:
    connect_database()

print("Connected to database successfully")
mycursor = mydb.cursor(buffered=True)
mycursor.execute("USE {}".format(config['connection']['dbname']))

faker = Faker()
def require_unique(options):
    if isinstance(options, dict) == False:
        return False
    return False if 'unique' not in options else bool(options['unique'])

def get_field_exception(options):
    if isinstance(options, dict) == False:
        return False
    return False if 'except' not in options else options['except']

def faking_data(options):
    type = options
    if isinstance(options, dict):
        type = options['type']

    if type is None:
        print ("Please specify type")
        sys.exit(2)

    if type == "email":
        return faker.profile('mail')['mail']
    elif type == "emailunique":
        return "{}.{}+{}@{}.com".format(faker.first_name().lower(), faker.last_name().lower(), faker.numerify(), slugify(faker.company()))
    elif type == "name":
        return faker.name()
    elif type == "first_name":
        return faker.first_name()
    elif type == "last_name":
        return faker.last_name()
    elif type == "phone_number":
        return faker.msisdn()
    elif type == "credit_card":
        return faker.credit_card_number()
    elif type == "address":
        return faker.address()
    elif type == "city":
        return faker.city()
    elif type == "postcode":
        return faker.postcode()
    elif type == "company_name":
        return faker.company()
    elif type == "job":
        return faker.profile('job')['job']
    elif type == "paragraph":
        return " ".join(faker.paragraphs(nb=1 if 'num' not in options else options['num']))
    elif type == "sentence":
        return " ".join(faker.sentences(nb=1 if 'num' not in options else options['num']))
    elif type == "isbn":
        return faker.isbn13(separator='-' if 'separator' not in options else options['separator'])
    elif type == "filename":
        return faker.file_name()
    elif type == "fileext":
        return faker.file_extension()
    elif type == "filepath":
        return faker.file_path()
    elif type == "mimetype":
        return faker.mime_type()
    elif type == "integer":
        return faker.random_int(min=1 if 'min' not in options else options['min'], max=9999 if 'max' not in options else options['max'], step=1 if 'step' not in options else options['step'])
    elif type == "float":
        return float(faker.random_int(min=1 if 'min' not in options else options['min'], max=9999 if 'max' not in options else options['max'], step=1 if 'step' not in options else options['step']))
    elif type == "choose":
        return faker.random_choices(elements=['Y', 'N'] if 'options' not in options else options['options'], length=1)[0]
    elif type == "fixed":
        return "fixed" if 'value' not in options else options['value']
    else:
        return faker.lexify(text='?????????? ?????????? ??????????')


for tablename in config['tables'].keys():
    print("Executing {} table".format(tablename))
    fields = []
    excepts = {}
    for fieldname in config['tables'][tablename].keys():
        fields.append(fieldname)
        exception = get_field_exception(config['tables'][tablename][fieldname])
        if exception:
            excepts[fieldname] = exception

    try:
        mycursor = mydb.cursor(buffered=True)
        sql_command = "SELECT id, {} FROM {} {}".format(','.join(fields), tablename, "" if not bool(excepts) else "WHERE {}".format("AND".join(['{} {}'.format(field, excepts[field]) for field in excepts.keys()])))
        mycursor.execute(sql_command)

        for row in mycursor:
            values = {}
            for fieldname in config['tables'][tablename].keys():
                values[fieldname] = faking_data(config['tables'][tablename][fieldname])

            mycursor = mydb.cursor(buffered=True)
            sql_command = "UPDATE {} SET {} WHERE id = {}".format(tablename, ",".join(['{}="{}"'.format(field, values[field]) for field in values.keys()]), row[0])
            mycursor.execute(sql_command)

        mydb.commit()
    except Error as e:
        print(e)