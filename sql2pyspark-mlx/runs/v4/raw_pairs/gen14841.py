from pyspark.sql import functions as F

eligible = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") >= F.col("s.order_date")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
        F.date_trunc("QUARTER", "order_date").alias("quarter_start"),
    )
    .agg(
        F.max("discount").alias("max_discount"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
    )
    .orderBy(F.col("max_discount").asc_nulls_last())
)
