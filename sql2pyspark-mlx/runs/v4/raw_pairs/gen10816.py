from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("shipping_fee_stddev").desc())

remaining_keys = (
    df_orders
    .select("status", "store_id")
    .intersect(
        df_orders
        .filter(F.col("shipping_fee") >= 18)
        .select("status", "store_id")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("quarter", "order_date").alias("quarter_start"),
        "customer_id",
        F.date_trunc("week", "order_date").alias("week_start"),
    )
    .agg(
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .withColumn("shipping_fee_stddev_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("shipping_fee_stddev").desc_nulls_last())
)
