from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Bangkok")
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "region",
        F.when(F.col("sqft") >= 15507, "high").when(F.col("sqft") >= 3143, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
    )
    .filter(F.col("new_york_count") >= 1)
    .orderBy("region", F.col("min_sqft").desc())
)
