"""
This module provides utility functions to select, insert and delete operations on delta tables.

The names and structure of tables, folders and paths do not have to conform to the library's standard convention.
"""
from .config import Config
from .utils import df_empty
from .dbfs_utils import directory_empty, table_exists, exists, get_empty_df
from .spark_init import get_spark_dbutils


def read(config, zone, path, **dataframe_reader_options):
  """Read all data, equivalent of doing a "SELECT *"

  Returns None if no data at path or path does not exists.
  Can still return a empty dataframe or a dataframe of empty rows if such is obtained from reading the files.

  Parameters
  ----------
  config: ETL.Config
  zone: str
  path: str
  dataframe_reader_options: dict

  Returns
  -------
  pyspark.sql.DataFrame
  """
  config.debug(message=path, func=read)
  spark, _ = get_spark_dbutils()
  zone = config.validate_zone_name(zone)
  config.mount_zone(zone, force=False)
  if not directory_empty(path):
    return spark.read.format('delta').load(path, **dataframe_reader_options)
  else:
    message = 'Returning empty dataframe since nothing at path {}'.format(path)
    config.debug(message=message, func=read)
    return get_empty_df()


def write(config, df, zone, path, table, **dataframe_writer_options):
  """Write/save data in the delta table(s).

  todo allow usage withouge needing to create tables

  Parameters
  ----------
  config: ETL.Config
    Config instance
  df: pyspark.sql.DataFrame
    Dataframe containing the data to save in the curated delta table.
  zone: str
  path: str
  table: str
  dataframe_writer_options: dict
  """
  config.debug(table=table, message=path, func=write)
  if df_empty(df):
    return
  zone = config.validate_zone_name(zone)
  config.mount_zone(zone, force=False)
  create_if_not_exists(config, table, schema=df.schema, location=path)
  df.write.format('delta').save(path, **dataframe_writer_options)


def merge(config, zone, table, df, path, unique_key):
  """Update data in the delta table.

  Performs a 'merge into'/'upsert' of a dataframe/table-view inside the Delata Table.
  Meaning rows are overwritten if it finds one with the same designated unqique fields, or else it is inserted.
  The batch of updates must not contain more than one row with the same chosen unique key.

  Handles the creation of the SQL condition update part. Needs an unique key, can be a combination of 2 or more fields.
  Eg. "ON delta_messages.message_id = updates.message_id AND delta_messages.id = updates.id"

  Parameters
  ----------
  config: ETL.Config
    Config instance
  zone: str
  table: str
  path: str
  df: pyspark.sql.DataFrame
    Dataframe containing the data to save in the curated delta table.
  unique_key: list or str
    Columns with unique values or list of columns to use as a combined unique key.
  """
  config.debug(table=table, message=path, func=merge)
  spark, _ = get_spark_dbutils()
  if df_empty(df):
    return
  zone = config.validate_zone_name(zone)
  config.mount_zone(zone, force=False)
  create_if_not_exists(config, table, schema=df.schema, location=path)
  unique_key = [unique_key] if isinstance(unique_key, str) else unique_key
  df.drop_duplicates(unique_key).createOrReplaceTempView('updates')

  update_condition = ''
  for i, field in enumerate(unique_key):
    conjunction = 'ON' if i == 0 else ' AND'
    update_condition += '{0} {1}.{2} = updates.{2}'.format(conjunction, table.lower(), field)

  spark.sql(
    "MERGE INTO {}.{} "
    "USING updates {} "
    "WHEN MATCHED THEN UPDATE SET * "
    "WHEN NOT MATCHED THEN INSERT *".format(config.data_source, table, update_condition))


def delete(config, zone, table, path, database_name=None, drop=False):
  """Delete delta data.

  Only use drop when really needed, otherwise just overwrite or delete.

  Parameters
  ----------
  config: ETL.Config
    Config instance
  zone: str
    Zone containing the tables to test.
  table
  path: str
  database_name: str, optional
    Defaults to config.data_source
  drop: bool, optional
    Drop table
  """
  config.debug(table=table, message=path, func=delete)
  spark, dbutils = get_spark_dbutils()
  zone = config.validate_zone_name(zone)
  config.mount_zone(zone, force=False)

  if not table_exists(table, config.data_source):
    return

  spark.sql('DELETE FROM {}.{}'.format(database_name or config.data_source, table))
  if drop is True:
    spark.sql('DROP TABLE IF EXISTS {}.{}'.format(database_name or config.data_source, table))
    dbutils.fs.rm(path, True)


def create_if_not_exists(config, table, schema, location=None):
  """Create table if not exists using data's schema

  Parameters
  ----------
  config: ETL.Config
    Config instance
  table: str
    Table's registered name in the metastore
  location: str, optional
  schema: pyspark.sql.types.StructType
  """
  config.debug(table=table, message=location, func=create_if_not_exists)
  spark, _ = get_spark_dbutils()
  location = "LOCATION '{}'".format(location) if location else ''
  if table_exists(table, config.data_source) and location:
    return

  spark.createDataFrame([], schema).createOrReplaceTempView('schema')
  spark.sql('CREATE DATABASE IF NOT EXISTS ' + config.data_source)
  spark.sql(
    "CREATE TABLE {}.{} "
    "USING DELTA {} "
    "AS SELECT * FROM schema".format(config.data_source, table, location))


def refresh_symlink(path):
  """Read a delta table at location to refresh the symlinks with the current secret.

  Databricks 6.1 runtime might be introducing a way to avoid this with its new python APIs for the delta.tables module.

  Unfortunatly, delta tables pointing to adls must be read with the spark api first to make them available again
  following a daily secret renewal. Since a table could be used at any time, this should be done before anything I/O related.

  This process typically involves some implicit reading logic using the SQL api when writing granuarly to a delta table (Eg. merge/upsert).
  (when spark append/write cant be used) It will fail if the databrick secret is not the same that at creation.

  Parameters
  ----------
    path: str
  """
  spark, _ = get_spark_dbutils()
  if not exists(path):
    raise FileNotFoundError('No delta table at provided location {}'.format(path))

  spark.read.format('delta').load(path).limit(1).collect()


def get_inserts_updates_deletes(config, zone, table, path, unique_key, database_name='default', old_version=None):
  """Returns data that was inserted, updated and deleted in the last delta table write operation.

  Needs a curated delta table that is overwritten with a full extract every loads.

  If ingesting data from the raw to curated with the function ETL.curated.write : OK
  If ingesting data from the raw to curated with the function ETL.curated.merge : NOT OK (breaks deletes)

  inserted: Based on a unique id, returns all rows with new id since last run.
  deleted:  Based on a unique id, returns all rows for which the id is no longer there since last run.
  updates:  Returns all rows with any changes in data, also checks in nested structures.

  Notes
  _____
  Does not support composed keys as unique_key

  Parameters
  ----------
  config: ETL.Config
  zone: str
  table: str
  path: str
  unique_key: str
  database_name: str, optional
    databricks db name
  old_version: int, optional
    If None, get the delta between the two last versions of the delta table.

  Returns
  -------
  (pyspark.sql.DataFrame, pyspark.sql.DataFrame or None, pyspark.sql.DataFrame or None)
  """
  config.debug(table=table, message=path, func=get_inserts_updates_deletes)
  spark, _ = get_spark_dbutils()
  zone = config.validate_zone_name(zone)
  config.mount_zone(zone, force=False)

  delta_version = spark.sql('SELECT max(version) FROM (DESCRIBE HISTORY {}.{})'.format(database_name, table)).collect()[0][0]

  if old_version is None:
    old_version = delta_version - 1
  elif old_version == delta_version:
    return None, None, None

  df = read(config, zone, path)

  if old_version < 1:
    return df, None, None

  old_df = read(config, 'curated', path, versionAsOf=old_version)

  insert_ids = df.select(unique_key).subtract(old_df.select(unique_key))
  inserts = df.join(insert_ids, unique_key, 'right')

  updates_and_inserts = df.subtract(old_df)
  updates = updates_and_inserts.join(insert_ids, unique_key, 'leftanti')

  delete_ids = old_df.select(unique_key).subtract(df.select(unique_key))
  deletes = old_df.join(delete_ids, unique_key, 'right')

  return inserts, updates, deletes


upsert = merge
