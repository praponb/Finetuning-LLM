from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.45, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
    )
    .agg(
        F.avg(F.when(F.col("status") == "COMPLETED", F.col("amount"))).alias("completed_avg_amount"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("shipping_fee_per_completed"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
    )
    .orderBy(F.col("completed_avg_amount").desc_nulls_last())
)
