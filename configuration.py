import os
from pyjavaproperties import Properties

CURRENT_DIR = os.getcwd()
DBMS_FILE = 'dbms.properties'

dbms_file_path = CURRENT_DIR + '/' + DBMS_FILE
properties = Properties()
properties.load(open(dbms_file_path))