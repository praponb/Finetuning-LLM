from pyspark.sql import functions as F

not_flagged = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") <= F.col("s.order_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy("channel")
    .agg(
        F.corr("amount", "discount").alias("corr_amount_discount"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
    )
    .orderBy("corr_amount_discount")
)
