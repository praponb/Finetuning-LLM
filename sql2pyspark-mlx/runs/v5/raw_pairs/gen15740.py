from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"))
    .filter(F.col("south_count") > 80)
)
