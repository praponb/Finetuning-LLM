from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.count(F.when(F.col("region") == "South", 1)).alias("south_count"))
    .filter(F.col("south_count") > 5)
    .select("city", "south_count")
)
