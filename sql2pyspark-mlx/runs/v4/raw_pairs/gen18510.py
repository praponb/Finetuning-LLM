from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
    )
    .filter(F.col("pct_west") >= 45)
)
