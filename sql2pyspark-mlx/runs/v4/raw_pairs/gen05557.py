from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(F.sum(F.when(F.col("country") == "JP", 1).otherwise(0)).alias("jp_count"))
)
