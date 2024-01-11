import findspark
findspark.init()
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, desc, monotonically_increasing_id, lower, regexp_replace, explode, udf, split,avg, length
from pyspark.sql import functions as F

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("ElasticsearchIndexCreation") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12-7.17.16.jar,commons-httpclient:commons-httpclient:3.1") \
    .config("spark.jars", "./elasticsearch-spark-30_2.12-7.12.0.jar,./commons-httpclient-3.1.jar") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.resource", "test/_doc") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()


ES_NODES = "elasticsearch"
ES_RESOURCE = "datatest/_doc"

file_path = "pplt.csv"
df = spark.read.csv(file_path)
df = df.dropna()
df2 = df.select("Time","Location", "PopTotal","PopMale","PopFemale") 
df2 = df2.filter(df["Time"] == 2018)
df2.orderby("PopTotal")
df2.limit(20)
json_df = df2.to_json()
json_df.write \
  .format("org.elasticsearch.spark.sql") \
  .option("es.nodes", "elasticsearch") \
  .option("es.resource", "population/_doc") \
  .option("es.mapping.id", "Location") \
  .option("es.write.operation", "upsert") \
  .option("es.index.auto.create", "true") \
  .option("es.nodes.wan.only", "true") \
  .mode("overwrite") \
  .save("population/_doc")

spark.stop()