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
mydata = spark.read.format("csv").option("header","true").load("original.csv")

from pyspark.sql.functions import *
cleaned_data = mydata.withColumn("clean_city", when(mydata.City.isNull(),"UNKNOWN").otherwise(mydata.City))
cleaned_data = cleaned_data.filter(cleaned_data.JobTitle.isNotNull())
cleaned_data = cleaned_data.withColumn("clean_salary", cleaned_data.Salary.substr(2,100).cast("float"))
mean = cleaned_data.groupBy().avg("clean_salary").take(1)[0][0]
print(mean)

from pyspark.sql.functions import lit
cleaned_data = cleaned_data.withColumn("new_salary", when(cleaned_data.clean_salary.isNull(), lit(mean)).otherwise(cleaned_data.clean_salary))
cleaned_data.show()

import numpy as np
latitudes = cleaned_data.select("Latitude")
latitudes = latitudes.filter(latitudes.Latitudes.isNotNull())
latitudes = latitudes.withColumn("latitude2",latitudes.Latitude.cast("float")).select("latitude2")
median = np.median(latitudes,collect())
cleaned_data = cleaned_data.withColumn("new_latitude", when(cleaned_data.Latitude.isNull(),lit(median)).otherwise(cleaned_data.Latitude))


