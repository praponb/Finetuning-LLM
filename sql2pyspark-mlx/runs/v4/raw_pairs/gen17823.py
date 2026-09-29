from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Rivers%") & (F.col("region") != "North") & F.col("sqft").isNotNull())
    .groupBy(
        "region",
        F.when(F.col("sqft") >= 12700, "high").when(F.col("sqft") >= 3525, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.max("opened_date").alias("latest_opened_date"),
        F.count("sqft").alias("sqft_count"),
    )
)
