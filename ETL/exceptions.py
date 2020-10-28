class NoTablesDefinedException(Exception):
  """Exception raised when trying to use a function without a sufficiently detailed
  set of params.

  The function is necessitating a Config instance with a set of params itself
  containing detailed informations about tables.

  """

  def __init__(self, func):
    message = ("Instanciate config with a set of params with more details in order "
               "to use {}.{}\nOr you may want to use lower level functions from "
               "json_utils or delta_utils modules instead.".format(func.__module__, func.__name__))
    self.message = message

  def __str__(self):
    return self.message
