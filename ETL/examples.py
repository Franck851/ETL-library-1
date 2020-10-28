"""
ETL.etl
~~~~~~~

This module provides examples use cases of moving and transforming data.

Different ways can be used depending on how standard the pipeline is.
"""
from . import raw_control, raw_tables, curated_tables

from .config import Config
from params import params
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import TimestampType

config = Config(params)


def parse_date(df):
  return df.withColumn(
    'date', to_timestamp(col('date').cast(TimestampType()), "yyyy-mm-dd'T'HH:mm:ss"))


curated_tables.merge(config, 'Messages', transformation=parse_date)


def raw_to_curated(table, transformation=None, incremental=False):

  # Read raw data, also retrieve potential control table updates
  df, short_paths = raw_tables.read(config, table, incremental=incremental)

  # Clean it
  df = transformation(df) if transformation else df

  # Merge into curated table
  curated_tables.merge(config, table, df)

  # Update control table
  raw_control.insert(config, short_paths)
