!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
!tar xf spark-3.1.1-bin-hadoop3.2.tgz
!pip install -q findspark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop3.2"
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.read.csv("challenge.csv",header = True)
from pyspark.sql.functions import *
df = df.withColumn("Is_Mexico",when(df.Country == "Mexico","Yes").otherwise("No"))
import pyspark.sql.functions as sqlfunc
sum_bytes = df.groupBy("Is_Mexico").agg(sqlfunc.sum("Bytes_used"))
sum_bytes.show()
df3 = df.groupBy("Country").agg(sqlfunc.countDistinct("ip_address").alias("ip_count"))
df3 = df3.sort(col("ip_count").desc()).show()

