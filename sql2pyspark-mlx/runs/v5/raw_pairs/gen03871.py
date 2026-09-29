from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum("sqft").alias("total_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("sqft_per_london"),
    )
)
