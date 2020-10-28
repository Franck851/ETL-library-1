from .spark_init import *
from .utils import df_empty, log
from pyspark.sql.utils import AnalysisException


def write_df(config, df, zone, path, num_files, **dataframe_writer_options):
  """Writes the spark dataframe in json format to the specified zone.

  Parameters
  ----------
  config: Config
    Config instance

  df: pyspark.sql.DataFrame
    Dataframe to write.

  zone: str
    ADLS zone to use in path.

  path: str

  num_files: int
    How many files to write.

  dataframe_writer_options
  """
  zone = config.validate_zone_name(zone)
  config.mount_zone(zone, force=False)
  dataframe_writer_options.setdefault('mode', 'overwrite')
  if not df_empty(df):
    df.repartition(num_files).write.mode('overwrite').json(
      path, timestampFormat="yyyy-MM-dd'T'HH:mm:ss", **dataframe_writer_options)


def read(config, zone, paths, **dataframe_reader_options):
  """Read all json files of the folder, equivalent of doing a "SELECT *"

  Returns None if no data at path or path does not exists.
  Can still return a empty dataframe or a dataframe of empty rows if such is obtained from reading the files.

  Parameters
  ----------
  config: Config
  zone: str
  paths: list or str
  dataframe_reader_options

  Returns
  -------
  pyspark.sql.DataFrame or None
  """
  zone = config.validate_zone_name(zone)
  config.mount_zone(zone, force=False)
  if isinstance(paths, list) and len(paths) == 0:
    log('List of paths is empty, nothing to read.')
  else:
    try:
      return spark.read.format('json').load(paths, **dataframe_reader_options)
    except AnalysisException as e:
      no_data = 'Unable to infer schema' in e.__str__()
      if no_data and isinstance(paths, str):
        log('Unable to infer schema from JSON, no data from files at location ' + paths)
      elif no_data and isinstance(paths, list):
        log('Unable to infer schema from JSON, no data from files at any locations, paths: ' + str(
          paths))
      else:
        raise e
