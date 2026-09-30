from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "South")
    .groupBy(
        F.when(F.col("region").isin(["North", "East"]), "group_a").otherwise("other").alias("region_group"),
    )
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.count("sqft").alias("sqft_count"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
    )
)
