from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Tow%") | F.col("sqft").between(11660, 13039))
    .groupBy(
        F.when(F.col("sqft") >= 14830, "high").when(F.col("sqft") >= 4746, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "South", 1).otherwise(0))) / F.count("*")), 1).alias("pct_south"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
    .orderBy(F.col("sqft_bucket").desc())
)
