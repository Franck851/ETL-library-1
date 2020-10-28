"""
This module provides utility functions to select/read/write JSON data from the trusted zone.

The names and structure of tables, folders and paths must conform to the library's standard convention:

  - trusted-zone -| - data_source - ...
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
import json

from . import dataframe_utils, curated, curated_control, delete
from .utils import build_path, df_empty
from .internal_utils import needs_params


@needs_params
def read(config, table, **dataframe_reader_options):
  """Read trusted zone's json files.

  Parameters
  ----------
  config: ETL.Config
    Config instance

  table: str

  dataframe_reader_options
  """
  config.debug(table=table, func=read)
  table = config.validate_table_name(table)
  path = build_path(config, 'trusted', table)
  dataframe_reader_options.setdefault('format', 'json')
  return dataframe_utils.read(config, 'trusted', path, **dataframe_reader_options)


@needs_params
def write(config, table, num_files=10, df=None, transformation=None,
          incremental: bool = None, **dataframe_writer_options):
  """Writes the spark dataframe in json format to the trusted zone.

  Parameters
  ----------
  config: ETL.Config
    Config instance

  table: str

  num_files: int
    How many files to write

  df: pyspark.sql.DataFrame
    Dataframe to write. Leave None to read table from curated.

  transformation: ETL.transform.Transform or (pyspark.sql.DataFrame) -> pyspark.sql.DataFrame, optional
    A function taking a df and returning a df.

  incremental: bool, optional
    To perform incremental load based on the curated-zone's control table content.
    Only possible when not passing your own DF.
    Have precedence over config's params

  dataframe_writer_options
  """
  config.debug(table=table, func=write)
  incremental = incremental if isinstance(incremental, bool) else config.params['tables'][table].get('incremental_trusted_mode', False)
  table = config.validate_table_name(table)
  dataframe_writer_options['mode'] = 'overwrite'
  dataframe_writer_options.setdefault('format', 'json')

  if not df:
    df = curated.read(config, table, incremental=incremental)

  if df_empty(df):
    if incremental:
      message = 'df is empty and incremental mode is set to true, deleting trusted zone data and returning.'
      delete(config, 'trusted', table, True)
    else:
      message = 'df is empty, returning.'

    config.debug(table=table, message=message, func=write)
    curated_control.update(config, table)
    return

  if transformation:
    message = 'Applying {} transformation'.format(transformation.transformation_function.__name__)
    config.debug(table=table, message=message, func=write)
    df = transformation(df)

  path = build_path(config, 'trusted', table)
  dataframe_utils.write(config, df, 'trusted', path, num_files=num_files, timestampFormat="yyyy-MM-dd'T'HH:mm:ss", **dataframe_writer_options)
  curated_control.update(config, table)
