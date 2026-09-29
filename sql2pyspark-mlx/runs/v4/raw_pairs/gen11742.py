from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy(F.col("status_length").desc(), "id")
w4 = Window.orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("store_id")
w6 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc())
w7 = Window.partitionBy("channel")
w8 = Window.partitionBy("store_id").orderBy("seq_no_share")
w9 = Window.orderBy(F.col("seq_no_share").desc_nulls_first())
w10 = Window.partitionBy("customer_id", "channel").orderBy(F.col("seq_no_share").desc())
w11 = Window.orderBy(F.col("amount_avg_2_2").asc_nulls_first())
w12 = Window.orderBy(F.col("discount").desc_nulls_first())

cleaned = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-06-26")))
    .select("*", F.length("status").alias("status_length"))
)

metrics = (
    cleaned
    .withColumn("corr_discount_amount", F.corr("discount", "amount").over(w))
    .withColumn("amount_avg_2", F.avg("amount").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("amount_avg_2_2", F.avg("amount").over(w4))
)

kept_keys = (
    metrics
    .select("customer_id")
    .intersect(
        df_orders
        .filter(F.col("order_date").between("2023-03-01", "2023-06-23"))
        .select("customer_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.corr("discount", "shipping_fee").over(w5).alias("corr_discount_shipping_fee"),
        F.cume_dist().over(w6).alias("shipping_fee_cume_dist"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w5)), 4).alias("seq_no_share"),
    )
)

metrics_orders = (
    enriched
    .withColumn("channel_max_shipping_fee_cume_dist", F.max("shipping_fee_cume_dist").over(w7))
    .withColumn("seq_no_share_cume_dist", F.cume_dist().over(w8))
    .withColumn("channel_min_seq_no_share", F.min("seq_no_share").over(w7))
    .withColumn("seq_no_share_dense_rank", F.dense_rank().over(w9))
    .withColumn("channel_avg_amount_avg_2_2", F.avg("amount_avg_2_2").over(w7))
)

result = (
    metrics_orders
    .select(
        "*",
        F.cume_dist().over(w10).alias("seq_no_share_cume_dist_2"),
        F.dense_rank().over(w11).alias("amount_avg_2_2_dense_rank"),
        F.cume_dist().over(w12).alias("discount_cume_dist"),
    )
)
