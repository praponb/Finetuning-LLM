from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc_nulls_first())
w3 = Window.partitionBy("store_id")
w_grp = Window.partitionBy("status", "store_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("order_date") >= F.to_date(F.lit("2023-07-19"))))
    .select(
        "*",
        F.substring("channel", 1, 4).alias("channel_prefix"),
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .withColumn("shipping_fee_dense_rank", F.dense_rank().over(w2))
    .withColumn("corr_discount_shipping_fee", F.corr("discount", "shipping_fee").over(w3))
)

metrics = (
    scored
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "shipping_fee_filled",
        "resolved_channel",
        "shipping_fee_dense_rank",
        F.sum("amount").over(w_grp).alias("last4_sum_amount"),
        F.min("amount").over(w_grp).alias("last4_min_amount"),
        F.max("amount").over(w_grp).alias("last4_max_amount"),
        F.count("amount").over(w_grp).alias("last4_count_amount"),
    )
)

result = (
    metrics
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .rollup(F.col("d.segment").alias("segment"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("src.last4_max_amount")), 2).alias("avg_last4_max_amount"),
        F.round(F.avg(F.col("src.shipping_fee_filled")), 2).alias("avg_shipping_fee_filled"),
    )
)
