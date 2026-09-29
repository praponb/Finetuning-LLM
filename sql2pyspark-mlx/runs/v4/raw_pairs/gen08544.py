from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 35, "LARGE").otherwise("SMALL").alias("shipping_fee_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
    )
    .orderBy(F.col("shipping_fee_tier").desc_nulls_last())
)
