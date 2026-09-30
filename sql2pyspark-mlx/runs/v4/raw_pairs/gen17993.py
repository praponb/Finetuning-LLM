from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "West", 1)), F.lit(0)), 2).alias("sqft_per_west"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("sqft_per_singapore"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
    .filter(F.col("sqft_per_west") >= 0)
    .orderBy(F.col("region").desc())
)
