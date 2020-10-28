"""
This module provides utility functions to select, insert and delete records of past incremental loads.

Table schema:
  (table_name STRING, last_version INT)

Example rows:
  inc-2020_01_10_22_35_33/Messages
  inc-2020_01_06_16_20_14/Messages
  inc-2019_10_30_13_33_24/Messages
  inc-2019_10_30_13_33_24/Groups
  inc-2020_01_10_22_35_33/Groups
  inc-2020_01_06_16_20_14/Topics
  inc-2019_12_13_17_40_35/Topics
  inc-2020_01_06_16_20_14/Groups
  inc-2020_01_13_13_32_08/Groups
  inc-2020_01_13_13_32_08/Users

Typical incremental flow:
  1. Read content of the control table
  2. Get the paths of raw-zone ingestion folders
  3. Filter folder paths for table that were not in the control table
  4. Get the paths of all files in the remaining folders
  5. Read JSON data with spark in one go by passing it the list of file paths
  6. If data ingestion is successful, insert folder paths in the control table

It is possible to operate on strings instead of registred table names to look for ingestion path patterns.
Eg. To delete from control table values with table name 'things' but in folder 'inc-2020' only ->
    ETL.raw_control(config, table_or_string='inc-2020/things')
"""
import re

from itertools import product
from .config import Config
from .utils import log, construct_sql_values
from .dbfs_utils import table_exists
from .spark_init import get_spark_dbutils


def select(config, table_or_string):
  """Get all control table values matching the table name or string pattern.

  Parameters
  ----------
  config: ETL.Config
  table_or_string

  Returns
  -------
  list
  """
  config.debug(table=table_or_string, message=config.curated_control_table_name, func=select)
  spark, _ = get_spark_dbutils()
  if not table_exists(config.raw_control_table_name, config.data_source):
    return []
  reg = build_regex_part(table_or_string)
  rows = spark.sql('SELECT DISTINCT path FROM {}.{} WHERE path RLIKE {}'.format(
    config.data_source, config.raw_control_table_name, reg)).toPandas()['path'].tolist()
  return rows


def insert(config, paths, table=None):
  """Insert paths in the raw-zone control table.

  Valid path example: 'inc-2019_07_18/Topics'

  Parameters
  ----------
  config: ETL.Config
    Config instance

  paths: list
    Valid DBFS paths to insert in the control table.

  table: str, optional
    Only for logging purposes.
  """
  config.debug(table=table, message=config.curated_control_table_name, list_data=paths, func=insert)
  spark, _ = get_spark_dbutils()
  create_if_not_exists(config)
  if len(list(paths)) == 0:
    return
  [path_is_valid(path, raise_when_invalid=True) for path in paths]
  values = construct_sql_values(paths)
  spark.sql('INSERT INTO TABLE {}.{} VALUES {}'.format(
    config.data_source, config.raw_control_table_name, values))


def delete(config, table_or_string_pattern):
  """Delete all control table rows containing table name or string pattern.

  Is case sensitive.

  Parameters
  ----------
  config: ETL.Config
    Config instance

  table_or_string_pattern: str
  """
  config.debug(table=table_or_string_pattern, message=config.raw_control_table_name, func=delete)
  spark, _ = get_spark_dbutils()
  if not table_exists(config.raw_control_table_name, config.data_source):
    log('Raw control table not yet created, returning.')
    return

  reg = build_regex_part(table_or_string_pattern)
  predicate = 'FROM {}.{} WHERE path RLIKE {}'.format(
    config.data_source, config.raw_control_table_name, reg)
  rows = spark.sql('SELECT * ' + predicate).collect()
  spark.sql('DELETE ' + predicate)

  if len(rows) > 0 and len(rows[0]) > 0:
    print('Deleted rows from raw-zone control table:\n', '\n'.join(['\t' + row[0] for row in rows]))


def path_is_valid(path, raise_when_invalid=False):
  if re.match('^[^/]+/.+[^/]$', path, flags=re.IGNORECASE):
    return True
  if not raise_when_invalid:
    return False
  raise ValueError('Invalid path format, paths must be formated exactly '
                   'like "inc-2019-06-13/table_name". Received: ' + path)


def create_if_not_exists(config, name=None):
  """Create control table if not exists.

  Save raw zone short paths, eg. "inc-2019-06-13/MessagesLikes"

  Parameters
  ----------
  config: ETL.Config
    Config instance
  name: str, optional
    If specified, name to use.
  """
  table_name = name or config.raw_control_table_name
  config.debug(table=table_name, func=create_if_not_exists)
  spark, _ = get_spark_dbutils()
  spark.sql('CREATE DATABASE IF NOT EXISTS ' + config.data_source)
  if not table_exists(config.raw_control_table_name, config.data_source):
    spark.sql('CREATE TABLE {}.{} (path STRING) USING DELTA'.format(config.data_source, table_name))
    config.curated_control_table_is_new = True

    # if table_exists(config.data_source + '_curated_crtl'):
    #   migrate_legacy_control_table(config)


def migrate_legacy_control_table(config):
  """Transfer data from the old control table if exists to the new one.

  The old table used to have only the "ingestion" table in it
  (eg. ".../inc-2019-06-13") but the new table is more granular.
  So when migrating, we simply add an entry for every table names
  eg. "inc-2019-06-13/MessagesLikes", "inc-2019-06-13/Users"... etc

  Some transformations are needed as the new control table values format
  allow for a more granular control:

    old format:
        "inc-2019-06-13"

    new format:
        "inc-2019-06-13/MessagesLikes"
        "inc-2019-06-13/Messages"
        "inc-2019-06-13/Users"

  Parameters
  ----------
  config: ETL.Config
    Config instance
  """
  spark, _ = get_spark_dbutils()
  log('Transferring data from old control table to the new one...')
  legacy_table_name = config.data_source + '_curated_crtl'
  old_inc_folders = spark.sql(
    'SELECT DISTINCT foldername FROM ' + legacy_table_name
  ).toPandas()['tablename'].tolist()
  iterable = product(old_inc_folders, config.tables)
  paths = [old_inc_folder + '/' + data_table for old_inc_folder, data_table in iterable]
  insert(config, paths)
  spark.sql('ALTER TABLE {} RENAME TO {}_legacy'.format(legacy_table_name, legacy_table_name))
  log('Transfer complete, old control table renamed to {}_legacy.'.format(legacy_table_name))
  config.debug(table=config.raw_control_table_name, message='Migrated legacy raw control table values',
               func=migrate_legacy_control_table, list_data=paths)


def build_regex_part(pattern):
  """Build SQL "WHERE path RLIKE -> regex_part <-" to match rows containing tables.

  Parameters
  ----------
  pattern: str
    Table to search or string like a specific folder with a table eg. "inc-2020-01-08/table"

  Returns
  -------
  str
  """
  return "'(^|/)({})($|/)'".format(pattern)


def path_to_short_path(path):
  """Convert any valid dbfs path to keep only the last part for the control table.

  '/dbfs/mnt/adls/raw_yammer/yammer/inc-2019_08_40/Messages'
  '/dbfs/mnt/adls/raw_yammer/yammer/inc-2019_08_40/Messages/file.json'
  to
  'inc-2019_08_40/Messages'
  'inc-2019_08_40/Messages'

  Parameters
  ----------
  path

  Returns
  -------
  str
  """
  end = -1 if '.' in path else None
  short_path = '/'.join(path.split('/')[-3:end]).rstrip('/')
  path_is_valid(short_path, raise_when_invalid=True)
  return short_path
