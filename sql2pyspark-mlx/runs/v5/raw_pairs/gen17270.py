from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") <= F.col("s.order_date")), "left_anti")
)

result = (
    kept
    .withColumn("amount_tier", F.when(F.col("amount") >= 1760, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["customer_id", "amount_tier"], ["customer_id"]],
        "customer_id",
        "amount_tier",
    )
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.grouping_id().alias("grouping_id"),
    )
)
