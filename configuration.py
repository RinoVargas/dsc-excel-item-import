import os
from pyjavaproperties import Properties

CURRENT_DIR = os.getcwd()
DBMS_FILE = '.env'

dbms_file_path = CURRENT_DIR + '/' + DBMS_FILE
properties = Properties()
properties.load(open(dbms_file_path))