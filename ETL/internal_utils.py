from functools import wraps


def extract_config_from_args(args, kwargs):
  try:
    from .config import Config
    return next(iter(a for a in args + tuple(kwargs.values()) if isinstance(a, Config)))
  except StopIteration as e:
    raise ValueError(
      'The decorated function needs a Config instance object in either args or kwargs') from e


def needs_params(f):
  @wraps(f)
  def _impl(*args, **kwargs):
    config = extract_config_from_args(args, kwargs)
    if config.with_limited_features:
      from exceptions import NoTablesDefinedException
      raise NoTablesDefinedException(f)
    return f(*args, **kwargs)

  return _impl
