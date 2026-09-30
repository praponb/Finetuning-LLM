from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.36, "LARGE").otherwise("SMALL").alias("discount_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("shipping_fee_per_returned"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
    )
)
