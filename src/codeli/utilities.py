"""This module contains regular utilities"""
import re
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType,StructField,ArrayType

def _rename_all(schema: StructType):
    schema_new = []
    for struct_field in schema:
        column_name = re.sub("[^0-9a-zA-Z]+","_",struct_field.name)
        if type(struct_field.dataType)==StructType:
            schema_new.append(StructField(column_name, StructType(_rename_all(struct_field.dataType)), struct_field.nullable, struct_field.metadata))
        elif type(struct_field.dataType)==ArrayType: 
            if type(struct_field.dataType.elementType)==StructType:
                schema_new.append(StructField(column_name, ArrayType(StructType(_rename_all(struct_field.dataType.elementType)),True), struct_field.nullable, struct_field.metadata)) # Recursive call to loop over all Array elements
            else:
                schema_new.append(StructField(column_name, struct_field.dataType.elementType, struct_field.nullable, struct_field.metadata)) # If ArrayType only has one field, it is no sense to use an Array so Array is exploded
        else:
            schema_new.append(StructField(column_name, struct_field.dataType, struct_field.nullable, struct_field.metadata))
    return schema_new

def rename_columns(schema: StructType):
    return StructType(_rename_all(schema))

def rename_columns_simple(data: DataFrame):
    return list(map(lambda x:  re.sub("[^0-9a-zA-Z]+","_",x), data.columns))

def create_delta_if_not_exists(
    spark: SparkSession,
    schema: StructType,
    table_name: str,
    location: str = None,
    partition_key: str = None):
    
    delta = DeltaTable.createIfNotExists(spark) \
        .tableName(table_name) \
        .addColumns(schema)

    if location:
        delta = delta.location(location)

    if partition_key:
        delta = delta.partitionedBy(partition_key.split(","))

    delta.execute()
