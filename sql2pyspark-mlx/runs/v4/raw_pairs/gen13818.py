from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(F.sum("age").alias("total_age"))
    .filter(F.col("total_age") > 260)
    .orderBy("city")
)
