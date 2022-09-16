"""This module contains the generic components"""
from typing import Dict
from pyspark.sql import DataFrame, SparkSession

from .dataset import (
    csv_read,
    delta_overwrite,
    json_read,
    xml_read,
    xls_read,
    xlsx_read,
    sql_read,
    delta_overwrite
)

from .utilities import (
    rename_columns,
    rename_columns_simple
)

from .transform import (
    add_column,
    add_current_timestamp,
    add_execution_id,
    add_row_hash,
    add_row_hash_structured,
    add_source_file_name,
    add_specific_timestamp,
    remove_duplicates
)

class Job:
    """This class is responsible for defining a Job.

    :param spark: A SparkSession object
    :type spark: class:`pyspark.sql.SparkSession`
    :param configuration: The configuration for the job
    :type configuration: Any
    :param log: A callback function for logging. Defaults to None
    :type log: function, optional
    """
    def __init__(self, spark: SparkSession, execution_id: str, log = None, **kwargs):
        self.spark = spark
        self.execution_id = execution_id
        self.log = log
        self.column_execution_id = kwargs.get("column_execution_id", "_execution_id_")
        self.column_row_hash = kwargs.get("column_row_hash", "_row_hash_")
        self.column_source_file = kwargs.get("column_source_file", "_source_file_")

    def execute(self, **kwargs):
        """This function executes the configuration of the transformation
        """
        dataset_source = kwargs["datasets"].pop("source")
        dataset_destination = kwargs["datasets"].pop("destination")
        transformations = kwargs.get("transformations")

        # Extracting source into dataframe
        data = self._ingest_source(**dataset_source)

        # Transforming dataframe
        if transformations:
            for step in transformations:
                data = self._transformation_step(data, step)

        # Loading dataframe into destination
        self._write_dataframe(data, dataset_destination)

    def _ingest_source(self, **source ) -> DataFrame:
        name = source.pop("name")
        path = source.pop("path")
        file_type = source.pop("type")
        properties = source.get("properties", {})

        if file_type == "csv":
            data = csv_read(self.spark, path, **properties)
            # Rename columns with special characters
            new_column_name_list= rename_columns_simple(data)
            data = data.toDF(*new_column_name_list)
            data = add_row_hash(data, self.column_row_hash)
        elif file_type == "json":
            data = json_read(self.spark, path, **properties)
            new_column_name_list= rename_columns(data.schema)
            data = self.spark.createDataFrame(data.rdd, new_column_name_list)
            data = add_row_hash_structured(data, self.column_row_hash)
        elif file_type == "xml":
            data = xml_read(self.spark, path, **properties)
            new_column_name_list= rename_columns(data.schema)
            data = self.spark.createDataFrame(
                data.rdd,
                new_column_name_list)
            data = add_row_hash_structured(data, self.column_row_hash)
        elif file_type == "xls":
            data = xls_read(self.spark, path, **properties)
            # Rename columns with special characters
            new_column_name_list=  rename_columns_simple(data)
            data = data.toDF(*new_column_name_list)
            data = add_row_hash(data, self.column_row_hash)
        elif file_type == "xlsx":
            data = xlsx_read(self.spark, path, **properties)
            # Rename columns with special characters
            new_column_name_list=  rename_columns_simple(data)
            data = data.toDF(*new_column_name_list)
            data = add_row_hash(data, self.column_row_hash)
        elif file_type == "sql":
            data = sql_read(self.spark, path)
            data = add_row_hash(data, self.column_row_hash)
        else:
            raise Exception("No valid file type was given.")

        self._write_log(
            "INFO",
            "ingest-source",
            f"Dataset {name} ({file_type}) loaded"
        )
        data = add_source_file_name(data, self.column_source_file)
        data = add_execution_id(data,self.execution_id, self.column_execution_id)
        return data

    def _transformation_step(self, data: DataFrame, step: Dict) -> DataFrame:
        step_name = step.pop("name")
        step_type = step.pop("type")

        self._write_log(
            "INFO",
            "transformation_step",
            f"Starting transformation {step_name} ({step_type})"
        )

        if step_type == "add_column":
            column_name = step["properties"].pop("column_name")
            default_value = step["properties"].get("default_value")

            return add_column(
                data = data,
                name = column_name,
                value = default_value
            )

        if step_type == "add_current_timestamp":
            column_name = step["properties"].pop("column_name")
            return  add_current_timestamp(
                data= data,
                column_name= column_name
            )

        if step_type == "add_specific_timestamp":
            column_name = step["properties"].pop("column_name")
            value = step["properties"].pop("timestamp")
            return add_specific_timestamp(
                data= data,
                column_name= column_name,
                timestamp= value
            )

        if step_type == "distinct":
            return remove_duplicates(data = data)

        raise Exception(f"Unknown transformation step. {step_type}")

    def _write_dataframe(self, data: DataFrame, destination: Dict):
        name = destination.pop("name")
        destination_type = destination.pop("type")
        path = destination.pop("path")
        properties = destination.get("properties", {})

        if destination_type == "overwrite":
            table = properties.pop("table")
            delta_overwrite(
                data,
                path,
                table
            )
        self._write_log(
            "INFO",
            "write_dataframe",
            f"Starting write of {name} ({destination_type})"
        )

    def _write_log(self, level, source, message):
        if self.log:
            self.log(level, source, message, self.execution_id)
