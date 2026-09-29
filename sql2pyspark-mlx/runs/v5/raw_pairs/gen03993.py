from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2024-03-08")
    .agg(F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"))
    .orderBy(F.col("west_count").desc())
)
