from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Cent%"))
    .rollup("city", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.countDistinct("sqft").alias("unique_citys"),
        F.count("sqft").alias("sqft_count"),
        F.round(((100 * F.sum(F.when(F.col("region") == "Central", 1).otherwise(0))) / F.count("*")), 1).alias("pct_central"),
    )
)
