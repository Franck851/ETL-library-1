from pyspark.sql.types import *
from .spark_init import get_spark_dbutils


def get_dbfs_mounts():
  _, dbutils = get_spark_dbutils()
  if hasattr(dbutils.fs, 'mounts'):
    return [mount.mountPoint for mount in dbutils.fs.__getattribute__('mounts')()]
  # dbconnect only: This sometimes leads to bugs trying to see if mount is mounted
  return [obj.path.split('dbfs:')[1].rstrip('/') for obj in dbutils.fs.ls('/mnt/adls/')]


def install_py_lib(library, version=None):
  """Install pypi library if on databricks and do not raise if called locally.

  Just a safe way to install python library for both databricks connect and notebooks.

  Parameters
  ----------
  library: str
  version: str, optional
  """
  _, dbutils = get_spark_dbutils()
  if hasattr(dbutils, 'library'):
    dbutils_library = dbutils.__getattribute__('library')
    if not any(library in lib for lib in dbutils_library.list()):
      if version is not None:
        dbutils_library.installPyPI(library, version)
      else:
        dbutils_library.installPyPI(library)


def table_exists(table_name, database_name='default'):
  """Check if table exists

  Parameters
  ----------
  table_name: str
  database_name: str, optional

  Returns
  -------
  bool
  """
  spark, _ = get_spark_dbutils()
  # Only check for table in database if said database exists, to not raise.
  return (
    len(spark.sql("SHOW DATABASES LIKE '{}'".format(database_name)).collect()) > 0 and
    len(spark.sql("SHOW TABLES IN {} LIKE '{}'".format(database_name, table_name)).collect()) > 0)


def get_empty_df(schema=None):
  """

  Parameters
  ----------
  schema: pyspark.sql.types.StructType, optional

  Returns
  -------
  pyspark.sql.DataFrame
  """
  spark, _ = get_spark_dbutils()
  return spark.createDataFrame([], schema or StructType())


def directory_empty(directory_path):
  """Check that the dir path exists and has files.

  Spark fails to read if either of those conditions aren't fulfilled.

  Parameters
  ----------
  directory_path: str

  Returns
  -------
  bool
  """
  _, dbutils = get_spark_dbutils()
  has_folder = any([e.name.strip('/') == directory_path.split('/')[-1]
                    for e in dbutils.fs.ls(directory_path + '/..')])
  has_files = len(dbutils.fs.ls(directory_path)) > 0
  return not has_folder or not has_files


def exists(path):
  _, dbutils = get_spark_dbutils()
  try:
    dbutils.fs.ls(path)
  except Exception as e:
    if 'FileNotFoundException' in e.__str__():
      return False
  return True


def clean_spark_meta_files(folder_path):
  """Delete metadata files that spark creates.

  Parameters
  ----------
  folder_path : string

  """
  _, dbutils = get_spark_dbutils()
  prefixes = ['_committed', '_started', '_SUCCESS']
  for file in dbutils.fs.ls(folder_path.replace('/dbfs', '').rstrip('/') + '/'):
    if any(prefix in file.path for prefix in prefixes):
      dbutils.fs.rm(file.path)
