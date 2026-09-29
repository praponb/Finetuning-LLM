from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 16340, "high").when(F.col("sqft") >= 6183, "mid").otherwise("low").alias("sqft_bucket"),
        F.when(F.col("city").isin(["London", "New York"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(F.min("opened_date").alias("min_opened_date"))
)
