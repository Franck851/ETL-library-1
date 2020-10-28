import os
import sys
import json
import random
import collections
import concurrent.futures

from pytz import timezone
from datetime import datetime

from .utils import log, suppress_stdout
from .spark_init import get_spark_dbutils
from .default_params import default_params
from .dbfs_utils import get_dbfs_mounts, exists


def mount_is_accessible(mount):
  """Check if the volume is ready.

  Use to avoid unecessarily mounting volumes.

  Parameters
  ----------
  mount: str

  Returns
  -------
  bool
  """
  spark, dbutils = get_spark_dbutils()
  if not exists(mount):
    return False
  try:
    with suppress_stdout():
      path = mount + '/test_file'
      dbutils.fs.rm(path, True)
      spark.createDataFrame([(1, 2)], ('a', 'b')).write.json(path, mode='overwrite')
  except:
    return False
  return True


class Config:
  supported_zones_data = ['raw', 'curated', 'trusted']
  supported_zones_test = ['raw_test', 'curated_test', 'trusted_test']
  supported_zones_all = supported_zones_data + supported_zones_test
  data_zone_to_test_zone_hash = dict(zip(supported_zones_data, supported_zones_test))  # {raw: raw_test, ...}
  test_zone_to_data_zone_hash = dict(zip(supported_zones_test, supported_zones_data))  # {raw_test: raw, ...}

  def __init__(self, params=None):
    self.with_limited_features = False

    if params is None:
      self.with_limited_features = True
      self.params = default_params
    else:
      self.params = params

    if 'excluded_raw_paths' not in self.params:
      self.params['excluded_raw_paths'] = []

    self.data_source = self.params['data_source']
    self.is_windows = sys.platform.startswith('win')
    self.raw_control_table_name = self.data_source + '_raw_control'
    self.curated_control_table_name = self.data_source + '_curated_control'
    self._adls_configs = {}
    self.tables = list(self.params['tables'].keys())
    self.IH_MOUNT_ADLS_RAW = '/mnt/adls/raw_' + self.data_source
    self.IH_MOUNT_ADLS_CURATED = '/mnt/adls/curated_' + self.data_source
    self.IH_MOUNT_ADLS_TRUSTED = '/mnt/adls/trusted_' + self.data_source
    self.IH_FS_AUTH_TYPE = os.environ['IH_FS_AUTH_TYPE']
    self.IH_FS_PROVIDER_TYPE = os.environ['IH_FS_PROVIDER_TYPE']
    self.IH_FS_ETL_SERVICE_PRINCIPAL_ID = os.environ['IH_FS_ETL_SERVICE_PRINCIPAL_ID']
    self.IH_DATABRICKS_SECRET_SCOPE = os.environ['IH_DATABRICKS_SECRET_SCOPE']
    self.IH_SECRET_KEY_FS_ETL_SERVICE_PRINCIPAL = os.environ['IH_SECRET_KEY_FS_ETL_SERVICE_PRINCIPAL']
    self.IH_FS_TENANT_ID = os.environ['IH_FS_TENANT_ID']
    self.IH_FS_ACCOUNT_NAME = os.environ['IH_FS_ACCOUNT_NAME']
    self.CLIENT_ENDPOINT = 'https://login.microsoftonline.com/' + self.IH_FS_TENANT_ID + '/oauth2/token'
    self.CONTAINER_RAW = 'abfss://ih-ingestion-rawzone@' + self.IH_FS_ACCOUNT_NAME + '.dfs.core.windows.net/'
    self.CONTAINER_CURATED = 'abfss://ih-curated-zone@' + self.IH_FS_ACCOUNT_NAME + '.dfs.core.windows.net/'
    self.CONTAINER_TRUSTED = 'abfss://ih-trusted-zone@' + self.IH_FS_ACCOUNT_NAME + '.dfs.core.windows.net/'
    self.debug_file = None

  @property
  def adls_configs(self):
    """Generates a dict of extra_configs arg needed for adls mounting.

    Returns
    -------
    dict
    """
    _, dbutils = get_spark_dbutils()
    ih_databricks_secret_scope = os.environ['IH_DATABRICKS_SECRET_SCOPE']
    ih_secret_key_fs_etl_service_principal = os.environ['IH_SECRET_KEY_FS_ETL_SERVICE_PRINCIPAL']
    client_secret = dbutils.secrets.get(
      scope=ih_databricks_secret_scope,
      key=ih_secret_key_fs_etl_service_principal,
    )
    return {
      'fs.azure.account.auth.type': self.IH_FS_AUTH_TYPE,
      'fs.azure.account.oauth.provider.type': self.IH_FS_PROVIDER_TYPE,
      'fs.azure.account.oauth2.client.id': self.IH_FS_ETL_SERVICE_PRINCIPAL_ID,
      'fs.azure.account.oauth2.client.secret': client_secret,
      'fs.azure.account.oauth2.client.endpoint': self.CLIENT_ENDPOINT,
    }

  def init_debug_file(self):
    """Create log file's directories, set file name and create new file.

    Returns
    -------
    str
      Name of the handler
    """
    self.mount_zone('raw', force=False)
    _, dbutils = get_spark_dbutils()

    if self.is_windows:
      file_name = 'C:/logs/{}/debug.json'.format(self.data_source)
      os.makedirs('/'.join(file_name.replace('\\\\', '/').split('/')[:-1]), exist_ok=True)
    else:
      file_name = '{}/logs/{}/debug.json'.format(self.IH_MOUNT_ADLS_RAW, self.data_source)
      dbutils.fs.mkdirs('/'.join(file_name.replace('\\\\', '/').split('/')[:-1]))
      file_name = '/dbfs' + file_name

    with open(file_name, 'w', encoding='utf-8') as f:
      json.dump([], f)

    return file_name

  def write_debug(self, obj):
    """Log to blob file in rawzone/logs

    Parameters
    ----------
    obj: dict
      object to add to json array log file
    """
    with open(self.debug_file, 'r+', encoding='utf-8') as f:
      content = json.load(f, object_pairs_hook=collections.OrderedDict)
      content.append(obj)
      f.seek(0)
      json.dump(content, f, ensure_ascii=False, indent=4)
      f.truncate()

  def debug(self, table=None, message=None, func=None, list_data=None, list_sample_size=1000):
    """Log message to file.

    Parameters
    ----------
    table: str
      Table / source

    message: str
      Message

    func: function, optional
      Function from where the debug statement originated as to extract its informations.

    list_data: list of str, optional
      List of strings, any collection of custom metadata to log.

    list_sample_size: int, optional
      When passing a list of strings to log, limit the size to this. (choosen randomly but uniformly)
    """
    self.mount_zone('raw', force=False)
    if not self.debug_file:
      self.debug_file = self.init_debug_file()
      log('Debug log file created at ' + self.debug_file)

    result = {
      'source': self.data_source,
      'time': datetime.now(timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
      'table': table,
      'callee': None if not func else '{}.{}'.format(func.__module__, func.__name__),
      'message': message,
      'list_data': None
    }

    if isinstance(list_data, list) and len(list_data) > 0:
      length = len(list_data)
      if length > list_sample_size:
        list_data = random.sample(list_data, list_sample_size)
        list_data.append('... {} of {} printed ...'.format(list_sample_size, length))
      result['list_data'] = list_data

    self.write_debug(result)

  def get_mount_name_from_zone_name(self, zone):
    """Get mount name from short zone name.

    Parameters
    ----------
    zone: str

    Returns
    -------
    str
    """
    if zone not in self.supported_zones_all:
      raise ValueError('Unknown zone {}, supported zones: {}'.format(
        zone, self.supported_zones_data))
    return getattr(self, 'IH_MOUNT_ADLS_' + zone.upper())

  def get_container_url_from_zone_name(self, zone):
    """Get container url from short zone name.

    Parameters
    ----------
    zone: str

    Returns
    -------
    str
    """
    if zone not in self.supported_zones_all:
      raise ValueError('Unknown zone {}, supported zones: {}'.format(
        zone, self.supported_zones_data))
    return getattr(self, 'CONTAINER_' + zone.upper())

  def validate_table_name(self, table):
    """Validate table name.

    Parameters
    ----------
    table: str

    Returns
    -------
    str
    """
    if not isinstance(table, str) or table not in self.tables:
      raise ValueError('Unsupported table {}, supported tables: {}'.format(table, self.tables))
    return table

  def validate_zone_name(self, zone):
    """Validate zone name.

    Parameters
    ----------
    zone: str

    Returns
    -------
    str
    """
    if not isinstance(zone, str) or zone not in self.supported_zones_data:
      raise ValueError('Unsupported zone {}, supported zones: {}'.format(
        zone, self.supported_zones_data))
    return zone

  def validate_table_names(self, tables=None):
    """Validate table names

    Parameters
    ----------
    tables: list or str

    Returns
    -------
    list
      Same table names as passed if they are valid or all possible table names if None.
    """
    if tables is None:
      return self.tables
    elif isinstance(tables, str) and tables in self.tables:
      return [tables]
    elif not isinstance(tables, list) or any([table not in self.tables for table in tables]):
      raise ValueError('Unsupported table {}, supported tables: {}'.format(tables, self.tables))
    return list(set(tables))

  def validate_zone_names(self, zones=None):
    """Validate zone names

    Parameters
    ----------
    zones: list or str

    Returns
    -------
    list
      Same zones names as passed if they are valid or all possible zone names if None.
    """
    if zones is None:
      return self.supported_zones_data
    elif isinstance(zones, str) and zones in self.supported_zones_data:
      return [zones]
    elif not isinstance(zones, list) or any(
      [zone not in self.supported_zones_data for zone in zones]):
      raise ValueError('Unsupported zone {}, supported zones: {}'.format(
        zones, self.supported_zones_data))
    return list(set(zones))

  def mount_zone(self, zone, force):
    """Mount a zone.

    Parameters
    ----------
    zone: str
      Name of the zone to mount.

    force: bool
      If True, always mount. If False, mount only if not accesible or not in dbutils.mounts.
    """
    _, dbutils = get_spark_dbutils()
    mount = self.get_mount_name_from_zone_name(zone)
    container = self.get_container_url_from_zone_name(zone)
    is_mounted = mount in get_dbfs_mounts()

    if force is False and is_mounted and mount_is_accessible(mount):
      return

    if not hasattr(dbutils.fs, 'mount'):
      msg = ('{} volume is not mounted (or inaccessible) and '
             'databricks connect cannot mount volumes.'.format(mount))
      log(msg)
      return

    if is_mounted:
      try:
        dbutils.fs.unmount(mount)
      except:
        log('Directory {} not mounted'.format(mount))

    log('Mounting volume: ' + mount)
    dbutils.fs.mount(source=container, mount_point=mount, extra_configs=self.adls_configs)

  def mount_zones(self, zones, force=True):
    """Mount every zones in a list. Tries mounting them concurrently to save time.

    Notes
    _____
    For now, it is unclear how much time, if any, is saved when starting mounts concurrently.

    Parameters
    ----------
    zones: list
      Name of the zones to mount.

    force: bool, optional
      If True, always mount. If False, mount only if not accesible or not in dbutils.mounts.
    """
    zones = self.validate_zone_names(zones)
    # noinspection PyTypeChecker
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(zones)) as executor:
      mounter = {executor.submit(self.mount_zone, zone, force=force): zone for zone in zones}
      for future in concurrent.futures.as_completed(mounter):
        res = mounter[future]
