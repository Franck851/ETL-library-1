"""
Common string formatting operations for W3C semantic standards.

"""
from pyspark.sql.functions import *
from pyspark.sql.types import *
import string


def format_week_days(col_name, lang):
  """From integer get weekday name in the specified language.

  Examples
  ________
  df.withColumn('DAY_OF_WEEK_NAME_FR', format_week_days('DAY_OF_WEEK_NUMBER', 'fr'))

  Parameters
  ----------
  col_name: str
  lang: str
    fr, en or es

  Returns
  -------
  pyspark.sql.column
  """
  id_name_dict = {
    '1': ('Dimanche', 'Sunday', 'Domingo'),
    '2': ('Lundi', 'Monday', 'Lunes'),
    '3': ('Mardi', 'Tuesday', 'Martes'),
    '4': ('Mercredi', 'Wednesday', 'Miércoles'),
    '5': ('Jeudi', 'Thursday', 'Jueves'),
    '6': ('Vendredi', 'Friday', 'Viernes'),
    '7': ('Samedi', 'Saturday', 'Sábado')
  }

  idx = 0
  if str.lower(lang) == 'en':
    idx = 1
  elif str.lower(lang) == 'es':
    idx = 2

  map_funcs = [when(col(col_name) == id, name[idx]) for id, name in id_name_dict.items()]

  return coalesce(*map_funcs)


def format_zip(col_name):
  """Add the missing space in a 6 character-long zip code.

  Examples
  ________
  df.withColumn('POSTAL_CODE', format_zip('POSTAL_CODE'))

  Parameters
  ----------
  col_name: str

  Returns
  -------
  pyspark.sql.column
  """
  return expr("concat(substr({0}, 0, 3), ' ', substr({0}, 4, 3))" .format(col_name))


def format_yes_no(col_name):
  """From, 0 ,'0', 'yes', 'oui', 'non', etc.. Get the true/false boolean equivalent.

  Examples
  ________
  df.withColumn('WITHDRAW_ONLY', format_yes_no('WITHDRAW_ONLY'))

  Parameters
  ----------
  col_name: str

  Returns
  -------
  pyspark.sql.column
  """
  bool_dict = {
    1: True, '1': True, 'yes': True, 'oui': True,
    0: False, '0': False, 'no': False, 'non': False
  }
  map_funcs = [when(trim(lower(col(col_name))) == word, bool_val) for word, bool_val in bool_dict.items()]
  return coalesce(*map_funcs)


def format_hour(col_name):
  """If hour is formatted like "9h00" get the W3C standard: "9:00:00"

  Examples
  ________
  df.withColumn('OPEN_HOUR', format_hour('OPEN_HOUR'))

  Parameters
  ----------
  col_name

  Returns
  -------
  pyspark.sql.column
  """
  return expr("concat(split({0}, 'h')[0], ':', split({0}, 'h')[1], ':', '00')".format(col_name))


def format_phone(col_name):
  """Formats a 10 digits number or takes the first 10 digits of a number.

  Parameters
  ----------
  col_name: str

  Returns
  -------
  pyspark.sql.column
  """
  return expr(
    "concat('+1-', substr({0}, 0, 3), '-', substr({0}, 4, 3), '-', substr({0}, 7, 4))"
    .format(col_name))


def format_transit_id(col_name):
  """Add a dash before the last character or digit.

  Parameters
  ----------
  col_name: str

  Returns
  -------
  pyspark.sql.column
  """
  return expr("concat(substr({0}, 0, length({0}) - 1), '-', split({0}, '')[length({0}) - 1])"
              .format(col_name))
