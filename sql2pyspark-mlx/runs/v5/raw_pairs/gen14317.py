from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "East")
    .groupBy(
        F.when(F.col("region").isin(["Central", "South"]), "group_a").otherwise("other").alias("region_group"),
        "region",
    )
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
    )
    .filter(F.col("max_sqft") < 11200)
)
