"""This module interacts with various datasets"""
from pyspark.sql import DataFrame, SparkSession
from delta import DeltaTable


def json_read(spark: SparkSession, source: str, **kwargs) -> DataFrame:
    """Reads a JSON file into a dataframe

    :param spark: A :class:`pyspark.sql.SparkSession` object.
    :type spark: :class:`pyspark.sql.SparkSession`
    :param source: The path to the json file or folder.
    :type source: str
    :keyword bool multiLine: When `True` reads the Json document with multiple lines. Defaults to `True`.
    :return: Returns a :class:`pyspark.sql.DataFrame` object.
    :rtype: DataFrame
    """

    # Set defaults
    mutli_line = kwargs.get("multiLine", True)
    data = spark.read.json(source, multiLine=mutli_line)
    return data


def csv_read(spark: SparkSession, source: str, **kwargs) -> DataFrame:
    """Reads a csv file into a dataframe

    :param spark: A :class:`pyspark.sql.SparkSession` object.
    :type spark: :class:`pyspark.sql.SparkSession`
    :param source: The path to the csv file or folder.
    :type source: str
    :keyword str Delimiter: The delimiter to use. Defaults to ,.
    :keyword bool Header: A `bool` to specifiy wheter or not to read the header. Defaults to `True`.
    :keyword bool EnforceShema: A `bool` to specify whether or not to enforce the schema. Defaults to `False`.
    :keyword bool InferSchema: A `bool` to indiate to read the schema. This requires an additional pass for the data. Defaults to `False`.
    :keyword str Quote: Specifies the charachter to use for quoting strings. Defaults to ".
    :keyword str Escape: Specifies the escape character. Defaults to \\.
    :keyword bool EscapeQuotes: Specifies if quotes should be escaped. Default to `True`.
    :keyword number SamplingRatio: Defines fraction of rows for schema inferring. Defaults to 1.0.
    :keyword bool MultiLine: Parse one record, which may span multiple lines, per file. Defaults to `True`.
    :return: Returns a :class:`pyspark.sql.DataFrame` object.
    :rtype: DataFrame
    """

    # Set defaults
    delimiter = kwargs.get("delimiter", ",")
    header = kwargs.get("header", True)
    enforce_schema = kwargs.get("enforceSchema", False)
    infer_schema = kwargs.get("inferSchema", False)
    quote = kwargs.get("quote", '"')
    escape = kwargs.get("escape", '\\')
    escape_quotes = kwargs.get("escapeQuotes", True)
    sampling_ratio = kwargs.get("samplingRatio", 1.0)
    multi_line = kwargs.get("multiLine", True)

    data = spark.read \
        .option("delimiter", delimiter) \
        .option("header", header) \
        .option("enforceSchema", enforce_schema) \
        .option("inferSchema", infer_schema) \
        .option("quote", quote) \
        .option("escape", escape) \
        .option("escapeQuotes", escape_quotes) \
        .option("samplingRatio", sampling_ratio) \
        .option("multiLine", multi_line) \
        .csv(source)
    return data


def sql_read(spark: SparkSession, source: str) -> DataFrame:
    """Loads a spark sql statement into a dataframe

    :param spark: A :class:`pyspark.sql.SparkSession` object.
    :type spark: :class:`pyspark.sql.SparkSession`
    :param source: The SQL statement to load.
    :type source: str
    :return: Returns a :class:`pyspark.sql.DataFrame` object.
    :rtype: DataFrame
    """
    data = spark.sql(source)
    return data


def delta_read(spark: SparkSession, source: str) -> DataFrame:
    """Loads a delta table into a dataframe

    :param spark: A :class: `pyspark.sql.SparkSession` object.
    :type spark: :class:`pyspark.sql.SparkSession`
    :param source: The path to the DeltaTable to read.
    :type source: str
    :return: Returns a :class:`pyspark.sql.DataFrame` object.
    :rtype: DataFrame
    """
    data = spark.read.format("delta").load(source)
    return data


def delta_overwrite(data: DataFrame, table: str, path: str) -> None:
    """Writes a dataframe to a delta table overwriting an existing table.

    :param data: The :class:`pyspark.sql.DataFrame` to write to a Delta table.
    :type data: :class:`pyspark.sql.DataFrame`
    :param table: The name of the managed table.
    :type table: str
    :keyword str Path: The path to write the table to. Defaults to `None`.
    """

    delta = data.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true")

    if path:
        delta = delta.option("path", path)

    delta.saveAsTable(table)


def delta_append(
        spark: SparkSession,
        source: DataFrame,
        destination: str,
        hash_column: str) -> None:
    """Takes an input delta table and add rows that do not exist or have changed. Rows are never updated, only added.

    :param spark: A :class:`pyspark.sql.SparkSession` object.
    :type spark: :class:`pyspark.sql.SparkSession`
    :param source: The :class:`pyspark.sql.DataFrame` to write to a Delta table.
    :type source: :class:`pyspark.sql.DataFrame`
    :param destination: The name of the Delta table.
    :type destination: str
    :param hash_column: The hash column that identifies the row.
    :type hash_column: str
    """
    dt_destination = DeltaTable.forPath(spark, destination)
    dt_destination.alias("dst").merge(
        source.alias("src"),
        f"dst.{hash_column}=src.{hash_column}") \
        .whenNotMatchedInsertAll() \
        .execute()


def delta_upsert(
        spark: SparkSession,
        source: DataFrame,
        destination: str,
        key_column: str,
        hash_column: str) -> None:
    """Takes an input delta table and merges the data with the destination table.

    :param spark: A :class:`pyspark.sql.SparkSession` object.
    :type spark: :class:`pyspark.sql.SparkSession`
    :param source: The :class:`pyspark.sql.DataFrame` to write to a Delta table.
    :type source: :class:`pyspark.sql.DataFrame`
    :param destination: The path to write the Delta table.
    :type destination: str
    :param key_column: The name of the key column.
    :type key_column: str
    :param hash_column: The name of the hash column that is used to identify changes to the row
    :type hash_column: str
    """

    dt_destination = DeltaTable.forPath(spark, destination)

    dt_destination.alias("dst").merge(
        source.alias("src"),
        f"dst.{key_column}=src.{key_column}") \
        .whenMatchedUpdateAll(f"src.{hash_column} != dst.{hash_column}") \
        .whenNotMatchedInsertAll() \
        .execute()


def delta_archive(
        spark: SparkSession,
        source: DataFrame,
        destination: str,
        key_column: str,
        **kwargs):
    """Takes an input delta table and transforms it based on the given key to a type 2 table

    :param spark: A :class:`pyspark.sql.SparkSession` object.
    :type spark: :class:`pyspark.sql.SparkSession`
    :param source: The :class:`pyspark.sql.DataFrame` to write to a Delta table.
    :type source: :class:`pyspark.sql.DataFrame`
    :param destination: The path to write the Delta table.
    :type destination: str
    :param key_column: The name of the key column.
    :keyword str hashColumn: Name of the hash column. Defaults to *_rowhash_*
    :keyword str closedDateColumn: The name of the column to use or create for the closed date.
    :keyword str closedDateValue: The value to use for the high date watermakr. Defaults to 9999-12-31.
    """

    hash_column = kwargs.get("hash_column", "_rowhash_")
    closed_date_column = kwargs.get("closed_date_column", "_closed_date_")
    closed_date_value = kwargs.get(
        "closed_date_value", "9999-12-31T23:59:59.999999+00:00")
    dt_destination = DeltaTable.forPath(spark, destination)

    df_new = source.alias("src") \
        .join(dt_destination.toDF().alias("dst"), key_column) \
        .where(
            f"dst.{closed_date_column}='{closed_date_value}' AND dst.{hash_column} <> src.{hash_column}") \
        .selectExpr('src.*')

    df_upd = (
        df_new
        .selectExpr("NULL as _merge_key", *source.columns)
        .union(source.selectExpr(f"{key_column} as _merge_key", *source.columns))
    )

    dt_destination.alias("dst").merge(
        df_upd.alias("upd"),
        f"dst.{key_column} = upd._merge_key") \
        .whenMatchedUpdate(
        condition=f"dst.{closed_date_column}='{closed_date_value}' AND dst.{hash_column} <> upd.{hash_column}",
        set={
            closed_date_column: "current_timestamp()"
        }
    ).whenNotMatchedInsertAll() \
        .execute()
