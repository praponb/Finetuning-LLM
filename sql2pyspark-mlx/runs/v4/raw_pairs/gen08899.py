from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("sqft_per_chiang_mai"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .filter(F.col("sqft_per_chiang_mai") >= 0)
)
