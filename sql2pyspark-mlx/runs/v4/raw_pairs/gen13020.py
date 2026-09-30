from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.41, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.percentile_approx("shipping_fee", 0.9).alias("shipping_fee_p90"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("shipping_fee_per_cancelled"),
    )
    .orderBy(F.col("avg_shipping_fee").desc_nulls_first())
)
