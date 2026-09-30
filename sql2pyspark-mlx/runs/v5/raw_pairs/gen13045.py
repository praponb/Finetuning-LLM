from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") >= 14)
    .groupBy("city", "region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
    )
    .filter(F.col("north_count") > 6)
    .select("city", "region", "avg_sqft", "north_count")
)
