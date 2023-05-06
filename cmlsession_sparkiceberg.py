
# Import PySpark
import pyspark
from pyspark.sql import SparkSession

#Create SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()


from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
schema = StructType([
  StructField("vendor_id", LongType(), True),
  StructField("trip_id", LongType(), True),
  StructField("trip_distance", FloatType(), True),
  StructField("fare_amount", DoubleType(), True),
  StructField("store_and_fwd_flag", StringType(), True)
])

dftaxi = spark.createDataFrame([], schema)
dftaxi.writeTo("marcsparkiceberg").create()

dftaxiproperties = spark.sql("show tblproperties default.marcsparkiceberg")
dftaxiproperties.show(35)




dfweblogs= spark.sql("select * from iceberg_weblogs")
dfweblogs.show(35)
