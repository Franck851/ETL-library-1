import json

from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import *
from .spark_init import get_spark_dbutils
from .utils import df_empty
from .dbfs_utils import get_empty_df


def write(config, df, zone, path, num_files=None, check_df=True, **dataframe_writer_options):
  """Writes the spark dataframe in json, csv, parquet or delta format to the specified zone.

  Parameters
  ----------
  config: ETL.Config
    Config instance
  df: pyspark.sql.DataFrame
  zone: str
    ADLS zone to use in path.
  path: str
  num_files: int, optional
    How many files to write.
  check_df: bool
    Dont check if df is empty before write, large execution plans can make the dataframewriter crash
  dataframe_writer_options
  """
  config.debug(message=path, func=write)
  if df is None or (check_df and df_empty(df)):
    message = 'Df is None or empty ({}). Other arguments: ' \
              'zone: {}, path: {}, num_files: {}'.format(type(df).__name__, zone, path, num_files)
    config.debug(message=message, func=write)
    return

  if dataframe_writer_options.get('format') == 'delta':
    raise NotImplementedError('Use delta_utils.write instead')  # todo table name = part of path?
    # from .delta_utils import write as write_delta
    # write_delta(config, df, zone, path)
    # return

  zone = config.validate_zone_name(zone)
  config.mount_zone(zone, force=False)

  if num_files is not None:
    df = df.repartition(num_files)

  df.write.save(path, **dataframe_writer_options)


def read(config, zone, path, **dataframe_reader_options):
  """Read all files of the folder, equivalent of doing a SELECT *

  CSV, JSON, parquet or delta.

  Parameters
  ----------
  config: ETL.Config
  zone: str
  path: list or str
  dataframe_reader_options:
    format: str

  Returns
  -------
  pyspark.sql.DataFrame
  """
  path_log = [path] if isinstance(path, str) else path
  config.debug(message=zone, func=read, list_data=path_log)
  spark, _ = get_spark_dbutils()
  zone = config.validate_zone_name(zone)
  config.mount_zone(zone, force=False)

  if isinstance(path, list) and len(path) == 0:
    message = 'Returning empty dataframe since read was passed an empty list of paths'
    config.debug(message=message, func=read, list_data=path)
    return get_empty_df()

  try:
    return spark.read.load(path, **dataframe_reader_options)
  # Spark's way to say to say there was no files or no files with anything to infer the schema
  except AnalysisException as e:
    if 'Unable to infer schema' in e.__str__():
      message = 'Unable to infer schema from JSON, no data from files at location {}'.format(path)
      config.debug(message=message, func=read)
      return get_empty_df()
    else:
      raise e


def groupby_and_to_list(df, groupby_col_name, new_col_name='x'):
  """Groupby column name and collect other columns as a list of structs.

  Examples
  ________

  >> atm_hours.show(truncate=False)
  +------+-----------+---------+----------+
  |ATM_ID|DAY_OF_WEEK|OPEN_HOUR|CLOSE_HOUR|
  +------+-----------+---------+----------+
  |1007  |1          |8:00:00  |23:00:00  |
  |1007  |2          |8:00:00  |23:00:00  |
  |1007  |3          |8:00:00  |23:00:00  |
  +------+-----------+---------+----------+

  >> atm_hours.printSchema()
  root
   |-- ATM_ID: string
   |-- DAY_OF_WEEK: string
   |-- OPEN_HOUR: string
   |-- CLOSE_HOUR: string
   |-- DAY_OF_WEEK: string

  >> atm_hours = groupby_and_to_list(atm_hours, 'ATM_ID', 'HOURS')

  >> atm_hours.show(truncate=False)
  +------+------------------------------------------------------------------------+
  |ATM_ID|HOURS                                                                   |
  +------+------------------------------------------------------------------------+
  |1007  |[[1, 8:00:00, 23:00:00], [2, 8:00:00, 23:00:00], [3, 8:00:00, 23:00:00]]|
  +------+------------------------------------------------------------------------+

  >> atm_hours.printSchema()
  root
   |-- ATM_ID: string
   |-- HOURS: array
   |    |-- element: struct
   |    |    |-- DAY_OF_WEEK: string
   |    |    |-- OPEN_HOUR: string
   |    |    |-- CLOSE_HOUR: string

  Parameters
  ----------
  df: pyspark.sql.DataFrame
  groupby_col_name: str
  new_col_name: str

  Returns
  -------
  pyspark.sql.DataFrame
  """
  col_names = [x for x in df.columns if x != groupby_col_name]
  agg_funcs = [collect_list(col_name).alias(col_name) for col_name in col_names]
  return (
    df
    .groupBy(groupby_col_name)
    .agg(*agg_funcs)
    .withColumn(new_col_name, arrays_zip(*col_names))
    .drop(*col_names)
  )
