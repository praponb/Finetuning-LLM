from pyspark.sql import functions as F

valid = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-04-17"))) & (~F.col("channel").eqNullSafe(F.lit("mobile"))))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.round("amount", 2).alias("amount_rounded"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.stddev_samp("discount").alias("discount_stddev"),
        F.percentile_approx("amount_rounded", 0.9).alias("amount_rounded_p90"),
        F.stddev_samp("discount").alias("discount_stddev_2"),
    )
    .orderBy(F.col("amount_rounded_p90").desc())
)
