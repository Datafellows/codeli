# CoDeLi
This project is a Python library to be used within a Spark context and runs common data engineering tasks 
from a configuration. 

There is a devcontainer available that loads a specific Spark and Delta version. To test this from a shell you need to start
up the correct version of delta.

To start PySpark 3.2.1 with Delta 1.2.1:

```pyspark --packages io.delta:delta-core_2.12:1.2.1 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"```