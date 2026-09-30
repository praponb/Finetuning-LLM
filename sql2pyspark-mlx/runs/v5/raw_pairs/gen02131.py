from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNull())
    .groupBy("city")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
    )
    .orderBy(F.col("city").desc())
)
