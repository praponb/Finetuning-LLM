from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower(F.trim("store_name")) == "mall central")
    .groupBy("region", "city")
    .agg(
        F.count("*").alias("total_count"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
    )
    .filter(F.col("total_count") > 9)
    .select("region", "city", "total_count", "south_count")
)
