"""
Functions to use the control table in wich is saved information about ingested curated-zone folders.

For an incremental loads or finding row-level updates between two points in time of a given table.

Table schema:
  (table_name STRING, last_version INT)

Example rows:
  MessagesLikes 1
  MessagesLikes	2
  Messages 1
  Messages 2
  Messages 3
  Groups	1

Typical incremental flow:
  1. read data from curated (current table state)
  2. look the control table for "table_name" and get the row with highest value of "version"
  3. read curated data from that point in time using the version from the control table
  4. substract (SQL except) that second df from the first one
  5. ingest/process the curated data
  6. if data was successfully processed, get the new "VERSION" from the table's metadata
  7. insert a row in the control table with (table_name, version)

"""
from .config import Config
from .dbfs_utils import table_exists
from .spark_init import get_spark_dbutils


def select(config, table):
  """Get last known delta table version saved inside the trusted control table.

  Get the version number metadata of the delta table that was last saved
  in the trusted control table so a diff of all updates since then can be retrieved.

  Parameters
  ----------
  config: ETL.Config
    Config instance

  table: str

  Returns
  -------
  str or None
  """
  config.debug(table=table, message=config.curated_control_table_name, func=select)
  if table_exists(config.curated_control_table_name, config.data_source):
    spark, _ = get_spark_dbutils()
    return spark.sql("SELECT MAX(last_version) FROM {}.{} WHERE table_name = '{}'".format(
      config.data_source, config.curated_control_table_name, table)).collect()[0][0]


def update(config, table):
  """Update the control table by inserting the delta table version id.

  Parameters
  ----------
  config: ETL.Config
    Config instance

  table: str
  """
  config.debug(table=table, message=config.curated_control_table_name, func=update)
  spark, _ = get_spark_dbutils()
  create_if_not_exists(config)
  latest_version = get_delta_table_version(config, table)
  spark.sql("INSERT INTO TABLE {}.{} VALUES ('{}', {})".format(
    config.data_source, config.curated_control_table_name, table, latest_version))


def get_delta_table_version(config, table):
  """Get the delta table version number from its name.

  Parameters
  ----------
  config: ETL.Config
    Config instance

  table: str

  Returns
  -------
  int
  """
  config.debug(table=table, message=config.curated_control_table_name, func=get_delta_table_version)
  spark, _ = get_spark_dbutils()
  return spark.sql('SELECT max(version) FROM (DESCRIBE HISTORY {}.{})'.format(
    config.data_source, table)).collect()[0][0]


def delete(config, table):
  """

  Parameters
  ----------
  config: ETL.Config
    Config instance

  table: str
  """
  config.debug(table=table, message=config.curated_control_table_name, func=delete)
  spark, _ = get_spark_dbutils()
  if not table_exists(config.curated_control_table_name, config.data_source):
    return
  query = "FROM {}.{} WHERE table_name = '{}'".format(
    config.data_source, config.curated_control_table_name, table)
  rows = spark.sql('SELECT * ' + query).collect()
  spark.sql('DELETE ' + query)

  if len(rows) > 0 and len(rows[0]) > 0:
    print('Deleted rows from curated control table:\n', '\n'.join(['\t' + row[0] for row in rows]))


def create_if_not_exists(config):
  """Create control table if not exists.

  Save delta tables versions.

  Parameters
  ----------
  config: ETL.Config
    Config instance
  """
  config.debug(table=config.curated_control_table_name, func=create_if_not_exists)
  spark, _ = get_spark_dbutils()
  spark.sql('CREATE DATABASE IF NOT EXISTS ' + config.data_source)
  if not table_exists(config.curated_control_table_name, config.data_source):
    spark.sql(
      'CREATE TABLE IF NOT EXISTS {}.{} ('
      'table_name STRING,'
      'last_version INT)'
      'USING DELTA'.format(config.data_source, config.curated_control_table_name))
