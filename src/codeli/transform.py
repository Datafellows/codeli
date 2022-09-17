"""This module contains the transformation components"""
import uuid
from typing import Any
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    md5, concat_ws,
    lit,
    input_file_name,
    to_json,
    struct)

def remove_duplicates(data: DataFrame, exclude: str = None) -> DataFrame:
    """Tranformation function"""
    if exclude:
        columns = [c for c in data.columns if c != exclude]
        return data.dropDuplicates(columns)

    return data.distinct()

def add_row_hash(data: DataFrame, name: str) -> DataFrame:
    """Tranformation function"""
    return _add_column_if_not_exists(data, name, md5(concat_ws(":", *data.columns)))

def add_row_hash_structured(data: DataFrame, name: str) -> DataFrame:
    """Tranformation function"""
    return _add_column_if_not_exists(data, name, md5(to_json(struct(*data.columns))))

def add_execution_id(data: DataFrame, execution_id: str, name: str) -> DataFrame:
    """Tranformation function"""
    return _add_column_if_not_exists(data, name, lit(execution_id))

def add_column(data: DataFrame, name: str, value: Any) -> DataFrame:
    """Tranformation function"""
    return _add_column_if_not_exists(data, name, lit(value))

def add_source_file_name(data: DataFrame, name: str) -> DataFrame:
    """Tranformation function"""
    return  _add_column_if_not_exists(data, name, input_file_name())

def add_key_hash(data: DataFrame, column_name: str, columns: str) -> DataFrame:
    """
    This function adds a hashed key column based on the contents of the specified columns.

    Parameters:
        data: The dataframe to add this column to. The dataframe should also include the columns.
        column_name: The name of the hashed key column. Defaults to _keyhash
        columns: A comma seperated list of columnnames.

    Returns:
        data: The dataframe with the added column and value.
    """

    # The keyhash is generated and placed as the first column.
    # This is needed because in some cases as a result
    # of the join the key column is put first.
    return data.select(md5(concat_ws(":", *columns.split(','))).alias(column_name), "*")

def add_current_timestamp(data: DataFrame, column_name: str) -> DataFrame:
    """Tranformation function"""
    data = _add_column_if_not_exists(data, column_name, current_timestamp())
    return data

def add_specific_timestamp(data: DataFrame, column_name: str, timestamp: str) -> DataFrame:
    """Tranformation function"""
    return _add_column_if_not_exists(
        data,
        column_name,
        lit(datetime.strptime(timestamp,'%Y-%m-%dT%H:%M:%S.%f%z')))

def add_uuid(data: DataFrame, column_name: str) -> DataFrame:
    """Tranformation function"""
    return _add_column_if_not_exists(data, column_name, lit(str(uuid.uuid4())))

def _add_column_if_not_exists(data: DataFrame, column_name: str, default_value: Any) -> DataFrame:
    """Tranformation function"""

    # If the column already exists return as fast as possible
    if column_name.upper() in (name.upper() for name in data.columns):
        return data

    return data.withColumn(column_name, default_value)
