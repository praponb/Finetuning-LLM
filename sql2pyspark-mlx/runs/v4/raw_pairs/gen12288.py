from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
    )
    .orderBy(F.col("distinct_customer_ids").desc_nulls_first())
)
