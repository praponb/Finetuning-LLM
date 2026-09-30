from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull() & F.month("opened_date").isin([3, 7, 11]) & (F.col("region") == "North"))
    .groupBy(
        "region",
        F.when(F.col("sqft") >= 17904, "high").when(F.col("sqft") >= 6057, "mid").otherwise("low").alias("sqft_bucket"),
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
    )
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .orderBy(F.col("sqft_bucket").desc())
)
