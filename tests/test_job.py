"""Module with Ingestion tests"""
import os
import json
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType)

from delta import DeltaTable, configure_spark_with_delta_pip

from codeli.job import Job
from codeli.dataset import delta_read

class TestIngest:
    """Class for ingestion tests"""
    builder = SparkSession.builder.appName("Tests") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    def _create_table(self, table_name, schema, path):
        """
        Test helper to create Delta table.
        """
        DeltaTable.createOrReplace(self.spark) \
            .tableName(table_name) \
            .addColumns(schema) \
            .addColumn("_rowhash", "STRING") \
            .addColumn("_execution_id", "STRING") \
            .addColumn("_create_date", "TIMESTAMP") \
            .addColumn("_is_current", "BOOLEAN") \
            .execute()

    def test_minimal(self):
        """
        Test to do a minimal load of data with no transformation
        """

        current_job = Job(self.spark, "test_minimal")
        current_job.execute(**get_configuration("tests/config/minimal.json"))

        df_test = DeltaTable.forName(spark, "test_case_1")
        assert df_test.count() == 1

def get_configuration(file_path: str):
    """Reads the json configuration from the source path."""
    with open(file_path, encoding='utf8') as json_file:
        return json.load(json_file)
