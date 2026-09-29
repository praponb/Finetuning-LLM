from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3400, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["store_id", "revenue_tier"], ["store_id"], []],
        "store_id",
        "revenue_tier",
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.sum("revenue").alias("total_revenue"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.count("*").alias("event_count"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("event_count").desc())
)
