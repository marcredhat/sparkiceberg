
# Import PySpark
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder\
  .appName("1.1 - Ingest") \
  .config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog") \
  .config("spark.sql.catalog.hivecat","org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.jars","https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.2.1/iceberg-spark-runtime-3.3_2.12-1.2.1.jar") \
  .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.hivecat.type","hadoop") \
  .config("spark.sql.catalog.spark_catalog.type","hive") \
  .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
  .config("spark.sql.catalog.hivecat.uri", "thrift://metastore-service.warehouse-ocp411pvc151-env-cdw-datalake-default.svc.cluster.local:9083") \
  .getOrCreate()


from pyspark.sql import functions as F
from pyspark.sql.functions import *

sdf = spark.createDataFrame([('3046-01-17 18:35:49', 'db1','metastore',100000)], ["time", "app","request","response_code"])
sdf = sdf.withColumn("time", F.col("time").cast("timestamp"))
sdf.withColumn("time",to_timestamp("time")).show(truncate=False)


sdf.toDF("time", "app","request","response_code").writeTo("default.ctas").using("iceberg").append()


df = spark.table("default.ctas").show()
