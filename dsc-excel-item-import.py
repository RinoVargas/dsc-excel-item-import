#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# SCRIPT
#

import logging
logger = logging.getLogger()

import json
import sys
import argparse
import os
import re
import pandas as pd
import sqlalchemy
from configuration import env


DESCRIPTION = """
Script for importing items into DSpace-CRIS from Excel files using DBMS Import framework
"""


def load(filename):
    '''Reads Excel file, validates mandatory columns, and returns an array of records as Python dicts.'''
    
    dfData = pd.read_excel(filename, dtype=object).dropna(how='all').fillna('')
    
    columns = dfData.columns.values
    assert columns[0] == 'ACTION'
    assert columns[1] == 'SOURCEREF'
    assert columns[2] == 'SOURCEID'
    assert columns[3] == 'collection'
    assert columns[-1] == 'NONE'
    assert len(columns) >= 4

    return dict(
        rows = dfData.to_dict('records'),
        metadata_fields = columns[4:-1]
    )


def db_connect():
    if env['DB_ENGINE'] != "postgresql":
        logger.error("DB Engine not yet supported: %s" % engine)
        raise NotImplementedError

    connString = '{engine}://{username}:{password}@{hostname}:{port}/{database}'
    connString = connString.format(
            engine=env['DB_ENGINE'],
            username=env['DB_USERNAME'],
            password=env['DB_PASSWORD'],
            hostname=env['DB_HOSTNAME'],
            port=env['DB_PORT'],
            database=env['DB_DATABASE'],
            )
    try:
        conn = sqlalchemy.create_engine(connString).connect()
        logger.debug('DB Connection established successfully.')
    except sqlalchemy.exc.OperationalError:
        logger.exception("Could not connect to DB.")
        raise
    
    return conn

def get_single_value_from_db(SQL, conn, default_value):
    dfRecord = pd.read_sql(SQL, conn)
    if len(dfRecord) != 1 or len(dfRecord.columns) != 1:
        raise ValueError('SQL query should return a single value')

    result = dfRecord.at[0,'max']

    if result is None: # max() returned a null value (no records)
        result = default_value

    return result

def get_db_data():
    conn = db_connect()

    SQL = """
    SELECT max(imp_id)
         FROM imp_record;
    """
    last_id = get_single_value_from_db(SQL, conn, 0)

    SQL = """
    SELECT max(imp_metadatavalue_id)
         FROM imp_metadatavalue;
    """
    last_metadatavalue_id = get_single_value_from_db(SQL, conn, 0)

    db_data = dict(
        last_id = last_id,
        last_metadatavalue_id = last_metadatavalue_id
    )
    
    conn.close()
    return db_data


def parse(data, db_data):
    records = []
    metadata_values = []
    cur_id = db_data['last_id']
    cur_metadatavalue_id = db_data['last_metadatavalue_id']

    for row in data['rows']:
        cur_id += 1
        assert row['collection'], 'Missing collection value in row: ' + row
        record = dict(
            imp_id = cur_id,
            imp_record_id = row['SOURCEID'],
            imp_sourceref = row['SOURCEREF'],
            imp_eperson_id = env['EPERSON_ID'],
            imp_collection_id = int(row['collection']),
            status = 'z',
            operation = row['ACTION'].lower()
        )
        if record['operation'] == 'update':
            for field in data['metadata_fields']:
                (schema, element, qualifier, lang) = parse_metadata_field_name(field.strip())
                for i, value in enumerate(str(row[field]).split('|||')):
                    cur_metadatavalue_id += 1
                    (display_value, authority, confidence) = parse_metadata_value(value)
                    metadata_value = dict(
                        imp_metadatavalue_id = cur_metadatavalue_id,
                        imp_id = cur_id,
                        imp_schema = schema,
                        imp_element = element,
                        imp_qualifier = qualifier,
                        imp_value = display_value,
                        imp_authority = authority,
                        imp_confidence = confidence,
                        metadata_order = i,
                        text_lang = lang
                    )
                    metadata_values.append(metadata_value)
        records.append(record)

    data = dict(
        records = records,
        metadata_values = metadata_values
    )
    return data


def parse_metadata_field_name(field):
    m = re.match(r"^(?P<schema>\w+)\.(?P<element>\w+)(\.(?P<qualifier>\w+))?(\[(?P<lang>\w+)\])?$", field)
    assert m is not None, 'Could not parse metadata info from: ' + field
    m = m.groupdict()
    assert m['schema'] is not None, 'Could not parse schema name from column: ' + field
    assert m['element'] is not None, 'Could not parse element name from column: ' + field
    return (
        m['schema'], 
        m['element'], 
        m['qualifier'] or '', 
        m['lang'] or ''
    )


def parse_metadata_value(value):
    m = re.match(r"^(\[CRISID=(?P<authority>\w+)\])?(?P<display_value>.*?)$", value)
    assert m is not None, 'Could not parse metadata value from: ' + value
    m = m.groupdict()
    confidence = 600 if m['authority'] else None
    return (
        m['display_value'], 
        m['authority'] or '',
        confidence
    )


def write_to_db(data):
    dfRecords = pd.DataFrame.from_dict(data['records'])
    logger.debug('imp_record :\n' + str(dfRecords))
    dfMetadataValues = pd.DataFrame.from_dict(data['metadata_values'])
    logger.debug('imp_metadatavalue :\n' + str(dfMetadataValues))

    conn = db_connect()
    
    try:
        dfRecords.to_sql('imp_record', conn, if_exists='append', index=False, chunksize=100)
        logger.debug('Wrote to imp_record successfully.')
    except sqlalchemy.exc.OperationalError:
        logger.exception("Could not write to imp_record.")
        raise

    try:
        dfMetadataValues.to_sql('imp_metadatavalue', conn, if_exists='append', index=False, chunksize=100)
        logger.debug('Wrote to imp_metadatavalue successfully.')
    except sqlalchemy.exc.OperationalError:
        logger.exception("Could not write to imp_metadatavalue.")
        raise

    conn.close()
    return data


def main(args, loglevel):
    logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)
    logger.debug("Filename: %s" % args.filename)
    logger.debug("Verbose: %s" % args.verbose)

    data = load(args.filename)
    db_data = get_db_data()
    data = parse(data, db_data)
    write_to_db(data)


def parse_args():

    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("filename",
                        metavar="FILENAME",
                        help="path to Excel file to import from")
    parser.add_argument("-v",
                        "--verbose",
                        help="increase output verbosity",
                        default=False,
                        action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.WARNING

    main(args, loglevel)
