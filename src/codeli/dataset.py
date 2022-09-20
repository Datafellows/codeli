"""This module interacts with various datasets"""
from pyspark.sql import DataFrame, SparkSession


def json_read(spark: SparkSession, source: str, **kwargs) -> DataFrame:
    """Reads a JSON file into a dataframe

    :param spark: A :class:`pyspark.sql.SparkSession` object.
    :type spark: :class:`pyspark.sql.SparkSession`
    :param source: The path to the json file or folder.
    :type source: str
    :keyword bool MultiLine: When `True` reads the Json document with multiple lines. Defaults to `True`.
    :return: Returns a :class:`pyspark.sql.DataFrame` object.
    :rtype: DataFrame
    """

    # Set defaults
    mutli_line = kwargs.get("MultiLine", True)
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
    delimiter = kwargs.get("Delimiter", ",")
    header = kwargs.get("Header", True)
    enforce_schema = kwargs.get("EnforceSchema", False)
    infer_schema = kwargs.get("InferSchema", False)
    quote = kwargs.get("Quote", '"')
    escape = kwargs.get("Escape", '\\')
    escape_quotes = kwargs.get("EscapeQuotes", True)
    sampling_ratio = kwargs.get("SamplingRatio", 1.0)
    multi_line = kwargs.get("MultiLine", True)

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

def delta_write(data: DataFrame, table: str) -> None:
    """Writes a dataframe into a (managed) table"""
    data.write.format("delta").saveAsTable(table)