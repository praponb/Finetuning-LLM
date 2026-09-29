from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("activity_month", F.date_trunc("MONTH", "order_date"))
    .cube("activity_month", "status")
    .agg(
        F.percentile_approx("shipping_fee", 0.9).alias("shipping_fee_p90"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("row_count").desc())
)
