import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as f 

spark = SparkSession.builder.appName("Data_Cleansing_").getOrCreate()

df_spark = spark.read.parquet("w2_input.parquet")

#Change Schema date String to datetime
df_spark = df_spark.withColumn("date",f.to_timestamp(df_spark.date))

#Create TempView
df_spark.createOrReplaceTempView("data")
df_sql = spark.sql("""SELECT\
                   transaction_id,\
                   date,\
                   CASE WHEN length(product_id) >5 THEN substr(product_id, 1, 5) ELSE product_id END as product_id ,\
                   price,\
                   customer_id,\
                   product_name,\
                   CASE WHEN customer_country = "Japane" THEN "Japan" ELSE customer_country END AS customer_country,\
                   customer_name,\
                   total_amount,\
                   thb_amount\
                   FROM data;""")

#Output
df_sql.write.parquet("data_output/with_sparksql_parquet")
df_sql.write.csv("data_output/with_sparksql_csv")
