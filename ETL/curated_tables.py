"""
This module provides utility functions to select, insert and delete operations on delta tables from the curated zone.

The names and structure of tables, folders and paths must conform to the library's standard convention:

  - curated-zone -| - data_source - ...
                  |
                  |
                  | - data_source - ...
                  |
                  |
                  | - yammer -  | - table_name
                                | - table_name
                                | - Messages
                                | - Users
                                | - Likes

"""
from . import raw_tables, curated_control, delta_utils, raw_control
from .config import Config
from .utils import build_path, df_empty
from .internal_utils import needs_params


@needs_params
def read(config, table, incremental=False, **dataframe_reader_options):
  """Read all data, equivalent of doing a "SELECT *"

  Returns None if no data at path or path does not exists.
  Can still return a empty dataframe or a dataframe of empty rows if such is obtained from reading the files.

  Parameters
  ----------
  config: ETL.Config
  table: str
  incremental: bool
    To perform incremental read based on the raw-zone's control table content.
  dataframe_reader_options

  Returns
  -------
  pyspark.sql.DataFrame
  """
  config.debug(table=table, func=read)
  table = config.validate_table_name(table)
  path = build_path(config, 'curated', table)
  full_df = delta_utils.read(config, 'curated', path, **dataframe_reader_options)
  old_version = curated_control.select(config, table)

  if not incremental or not old_version:
    message = 'Returning full curated data dataframe ({} rows)'.format(full_df.count())
    config.debug(table=table, message=message, func=read)
    return full_df

  # DF from the last time it was written in the trusted zone
  old_df = delta_utils.read(config, 'curated', path, versionAsOf=old_version, **dataframe_reader_options)
  updates = full_df.subtract(old_df)
  message = 'Returning the incremental extract of curated data updates ' \
            'since last run.Read {} new curated rows'.format(updates.count())
  config.debug(table=table, message=message, func=read)

  return updates


@needs_params
def write(config, table, df=None, transformation=None, incremental=False,
          raw_dataframe_reader_options=None, **dataframe_writer_options):
  """Write/save data in the delta table.

  Parameters
  ----------
  config: ETL.Config
    Config instance
  table: str
    Name of the table / folder.
  df: pyspark.sql.DataFrame, optional
    Dataframe containing the data to save in the curated delta table.
  transformation: ETL.transform.Transform or (pyspark.sql.DataFrame) -> pyspark.sql.DataFrame, optional
    A function taking a df and returning a df.
  incremental: bool, optional
    To perform incremental load based on the raw-zone's control table content.
  raw_dataframe_reader_options: dict, optional
    If df is not None, options are used when reading raw zone files.
  dataframe_writer_options:
      mode: str
          'overwrite' or 'append'
  """
  config.debug(table=table, func=write)
  table = config.validate_table_name(table)
  raw_dataframe_reader_options = raw_dataframe_reader_options or {}
  dataframe_writer_options.setdefault('mode', 'overwrite')
  short_paths = []

  if not df:
    df, short_paths = raw_tables.read(config, table, incremental=incremental, **raw_dataframe_reader_options)

  if df_empty(df):
    config.debug(table=table, message='df is empty, returning.', func=write)
    raw_control.insert(config, short_paths, table)
    return

  if transformation:
    message = 'Applying {} transformation'.format(transformation.transformation_function.__name__)
    config.debug(table=table, message=message, func=write)
    df = transformation(df)

  path = build_path(config, 'curated', table)
  delta_utils.write(config, df, 'curated', path, table, **dataframe_writer_options)
  raw_control.insert(config, short_paths, table)


@needs_params
def merge(config, table, df=None, transformation=None, unique_key=None,
          incremental: bool = None, raw_dataframe_reader_options=None):
  """Update data in the delta table.

  If df is not provided, read table data from raw.

  Performs a 'merge into'/'upsert' of a dataframe/table-view inside the Delta Table.
  Meaning rows are overwritten if it finds one with the same designated unqique fields, or else it is inserted.
  The batch of updates must not contain more than one row with the same chosen unique key.

  Handles the creation of the SQL condition update part. Needs an unique key, can be a combination of 2 or more fields.
  Eg. "ON delta_messages.message_id = updates.message_id AND delta_messages.id = updates.id"

  Parameters
  ----------
  config: ETL.Config
    Config instance
  table: str
    Name of the table / folder.
  df: pyspark.sql.DataFrame, optional
    Dataframe containing the data to save in the curated delta table.
  transformation: ETL.transform.Transform or (pyspark.sql.DataFrame) -> pyspark.sql.DataFrame, optional
    A function taking a df and returning a df.
  unique_key: list or str, optional
    Columns with unique values or list of columns to use as a combined unique key.
  incremental: bool, optional
    To perform incremental load based on the raw-zone's control table content. Only when not passing own df.
  raw_dataframe_reader_options: dict, optional
    If df is not None, options are used when reading raw zone files.
  """
  config.debug(table=table, func=merge)
  table = config.validate_table_name(table)
  raw_dataframe_reader_options = raw_dataframe_reader_options or {}
  short_paths = []

  if not df:
    df, short_paths = raw_tables.read(config, table, incremental=incremental, **raw_dataframe_reader_options)

  if df_empty(df):
    config.debug(table=table, message='df is empty, returning.', func=merge)
    raw_control.insert(config, short_paths, table)
    return

  if transformation:
    message = 'Applying {} transformation'.format(transformation.transformation_function.__name__)
    config.debug(table=table, message=message, func=merge)
    df = transformation(df)

  unique_key = unique_key or config.params['tables'][table]['unique_key']
  path = build_path(config, 'curated', table)
  delta_utils.merge(config, 'curated', table, df, path, unique_key)
  raw_control.insert(config, short_paths, table)


@needs_params
def delete(config, table, drop=False):
  """Delete delta data.

  Only use drop when really needed, otherwise just overwrite or delete.

  Parameters
  ----------
  config: ETL.Config
    Config instance
  table: str or list, optional
    Table name
    Leave empty to use all tables.
  drop: bool, optional
    Drop table
  """
  config.debug(table=table, func=delete)
  table = config.validate_table_name(table)
  path = build_path(config, 'curated', table)
  delta_utils.delete(config, 'curated', table, path, drop=drop)


def get_inserts_updates_deletes(config, table):
  """Returns data that was inserted, updated and deleted in the last delta table write operation.

  Needs a standard curated delta table that is overwritten with a full extract every loads.

  If ingesting data from the raw to curated with the function ETL.curated.write : OK
  If ingesting data from the raw to curated with the function ETL.curated.merge : NOT OK (breaks deletes)

  inserted: Based on a unique id, returns all rows with new id since last run.
  deleted:  Based on a unique id, returns all rows for which the id is no longer there since last run.
  updates:  Returns all rows with any changes in data, also checks in nested structures.

  Parameters
  ----------
  config: ETL.Config
  table: str

  Returns
  -------
  (pyspark.sql.DataFrame, pyspark.sql.DataFrame or None, pyspark.sql.DataFrame or None)

  Notes
  _____
  Either update curated control table manually after usage or use trusted_tables.write
  to ensure that the next delta reflects the new changes.
  """
  config.debug(table=table, func=write)
  table = config.validate_table_name(table)
  path = build_path(config, 'curated', table)

  return delta_utils.get_inserts_updates_deletes(
    config,
    'curated',
    table,
    path,
    config.params['tables'][table]['unique_key'],
    database_name=config.data_source,
    old_version=curated_control.select(config, table)
  )

upsert = merge
