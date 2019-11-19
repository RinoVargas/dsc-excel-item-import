import os
from pyjavaproperties import Properties

CURRENT_DIR = os.getcwd()
ENV_FILE = '.env'

# Set env file location
env_file_path = CURRENT_DIR + '/' + ENV_FILE

# Instance of Properties Object
envProperties = Properties()

# Load properties from .env file
envProperties.load(open(env_file_path))

# Set dictionary of properties
env = envProperties.getPropertyDict()