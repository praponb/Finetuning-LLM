from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("paid_at_hour").desc_nulls_first())
w4 = Window.partitionBy("week_start").orderBy(F.col("avg_shipping_fee").desc())

deduped = (
    df_payments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    deduped
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "rank_in_group",
        F.hour("paid_at").alias("paid_at_hour"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
)

deduped_payments = (
    latest
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped_payments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.channel"), F.col("p.shipping_fee"), F.col("p.order_date"))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        "channel",
        F.when(F.col("paid_at_hour") >= 77, "HIGH").otherwise("LOW").alias("paid_at_hour_tier"),
    )
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.max(F.when(F.col("status") == "SUCCESS", F.col("shipping_fee"))).alias("success_max_shipping_fee"),
    )
    .filter(F.col("avg_shipping_fee") >= 0)
    .withColumn("avg_shipping_fee_dense_rank", F.dense_rank().over(w4))
)
