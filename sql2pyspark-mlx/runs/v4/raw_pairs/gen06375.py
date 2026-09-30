from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("amount_tier", F.when(F.col("amount") >= 1750, "LARGE").otherwise("SMALL"))
    .cube("customer_id", "amount_tier")
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.grouping("customer_id").alias("grouping_level"),
    )
    .orderBy(F.col("shipping_fee_stddev").desc_nulls_last())
)
