from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "London")
    .groupBy("region")
    .agg(F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"))
)
