"""
This module provides utility functions to select/read JSON data from the ingestion raw zone.

The names and structure of tables, folders and paths must conform to the library's standard convention:

  - raw-zone -| - data_source - ...
              |
              |
              | - data_source - ...
              |
              |
              | - yammer -  |
                            |
                            | - ingestion_date -| - table_name
                            |                   | - table_name
                            |
                            | - ingestion_date -| - table_name
                            |                   | - table_name
                            |
                            | - ingestion_date -| - table_name
                            |
                            |
                            | - inc-2019-12-31 -| - Users
                            |                   | - Messages
                            |
                            | - inc-2020-01-01 -| - Messages
                                                | - Groups


Data cannot be deleted/updated.
"""
from . import dataframe_utils, raw_control
from .config import Config
from .spark_init import get_spark_dbutils
from .internal_utils import needs_params


@needs_params
def read(config, table, incremental=False, **dataframe_reader_options):
  """

  Parameters
  ----------
  config: ETL.Config
  table: str
  incremental: bool
  dataframe_reader_options

  Returns
  -------
  (pyspark.sql.DataFrame, list)
  """
  config.debug(table=table, func=read)
  table = config.validate_table_name(table)
  dataframe_reader_options.setdefault('format', 'json')

  # Handle the original spark dataframereader option 'path'
  if 'path' not in dataframe_reader_options:
    raw_zone_file_paths, control_table_updates = get_table_file_paths(config, table, incremental=incremental)
  else:
    raw_zone_file_paths = dataframe_reader_options.pop('path')
    raw_zone_file_paths = [raw_zone_file_paths] if isinstance(raw_zone_file_paths, str) else raw_zone_file_paths
    control_table_updates = [raw_control.path_to_short_path(path) for path in raw_zone_file_paths]

  df = dataframe_utils.read(config, 'raw', raw_zone_file_paths, **dataframe_reader_options)

  raw_count_log = len(raw_zone_file_paths) if isinstance(raw_zone_file_paths, list) else 1
  message = 'Read {} new raw files for {} new rows'.format(raw_count_log, df.count())
  config.debug(table=table, message=message, func=read, list_data=raw_zone_file_paths)

  return df, control_table_updates


@needs_params
def get_table_file_paths(config, table, incremental=False):
  """Get all absolute paths of files with data for the provided table name across all incremental folders.

  Also returns short paths ready to update the raw control table.

  Return format:

  ( [table_file_paths, ...], [control_table_updates, ....] )

  (
    [
      '/dbfs/raw_mount/data_source/ingestion_date/table_name/some_file.json',
      '/dbfs/raw_mount/data_source/ingestion_date/table_name/some_file.json',
      '/dbfs/raw_mount/data_source/ingestion_date/table_name/some_file.json'
    ],
    [
      'inc-2019_09_08/table_name',
      'inc-2019_12_12/table_name'
    ]
  )

  Parameters
  ----------
  config: ETL.Config
  table: str
  incremental: bool

  Returns
  -------
  (list of str, list of str)
  """
  config.debug(table=table, func=get_table_file_paths)
  _, dbutils = get_spark_dbutils()
  config.mount_zone('raw', force=False)
  table = config.validate_table_name(table)
  table_paths, control_table_updates = get_table_folder_paths(config, table, incremental=incremental)
  # Since glob.blob doesn't work from databricks connect, we cannot use wildcard patterns.
  file_paths = [y.path for x in table_paths for y in dbutils.fs.ls(x)]
  return file_paths, control_table_updates


@needs_params
def get_table_folder_paths(config, table, incremental=False):
  """Get all absolute paths of folders with data for the provided table name across all incremental folders.

  Also returns short paths ready to update the raw control table.
  Batching paths discovery by folders may also reduce chances of timeout.

  Return format:

  ( [table_folder_paths, ...], [control_table_updates, ....] )

  (
    [
      '/dbfs/raw_mount/data_source/inc-2019_09_08/table_name',
      '/dbfs/raw_mount/data_source/inc-2019_12_12/table_name
    ],
    [
      'inc-2019_09_08/table_name',
      'inc-2019_12_12/table_name'
    ]
  )

  Parameters
  ----------
  config: ETL.Config
  table: str
  incremental: bool

  Returns
  -------
  (list of str, list of str)
  """
  config.debug(table=table, func=get_table_folder_paths)
  _, dbutils = get_spark_dbutils()
  table = config.validate_table_name(table)
  config.mount_zone('raw', force=False)
  final_table_folder_paths, control_table_updates = [], []
  control_table_paths = raw_control.select(config, table)
  mount = config.get_mount_name_from_zone_name('raw')

  # Since glob.blob doesn't work from databricks connect,
  # we cannot use wildcard patterns.
  # (It would be: '/dbfs/mnt/adls/raw/data_source/*/table_name')
  inc_folders = [x.path for x in dbutils.fs.ls('{}/{}/'.format(mount, config.data_source))]
  table_folder_paths = [y.path for x in inc_folders for y in dbutils.fs.ls(x) if y.name[:-1] == table]

  for table_path in table_folder_paths:
    short_table_path = raw_control.path_to_short_path(table_path)
    in_control_table = any(short_table_path in control_table_path for control_table_path in control_table_paths)
    excluded = any(excluded_raw_path in table_path for excluded_raw_path in config.params['excluded_raw_paths'])

    if not excluded and (not incremental or not in_control_table):
      final_table_folder_paths.append(table_path)
      if not in_control_table:
        control_table_updates.append(short_table_path)

  return final_table_folder_paths, control_table_updates
