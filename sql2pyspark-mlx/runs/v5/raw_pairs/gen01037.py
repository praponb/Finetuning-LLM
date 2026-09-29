from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy(F.col("order_amount_2").desc_nulls_last())
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("channel").orderBy("amount", "id")
w4 = Window.partitionBy("quarter_start").orderBy(F.col("num_order_status").desc())

joined = (
    df_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.status",
        F.col("o.amount").alias("order_amount"),
        F.col("o.status").alias("order_status"),
        F.col("o.channel"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.shipping_fee") <= 35)
    .select(
        "src.*",
        F.col("d.channel").alias("order_channel"),
        F.col("d.shipping_fee"),
        F.col("d.amount"),
    )
)

enriched = (
    with_dims
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.amount").alias("order_amount_2"),
        F.col("b.order_date"),
        F.col("b.shipping_fee").alias("order_shipping_fee"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "order_id",
        "order_amount",
        "order_status",
        "shipping_fee",
        "order_date",
        "order_shipping_fee",
        F.rank().over(w).alias("order_amount_2_rank"),
        F.lead("order_amount_2", 1).over(w2).alias("next_order_amount_2"),
        F.lead("order_shipping_fee", 1).over(w3).alias("next_order_shipping_fee"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("QUARTER", "order_date").alias("quarter_start"),
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
    )
    .agg(
        F.count_if(F.col("order_status") == "RETURNED").alias("returned_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("order_amount").alias("max_order_amount"),
        F.percentile_approx("order_amount_2_rank", [0.25, 0.5, 0.75]).alias("order_amount_2_rank_quartiles"),
        F.count(F.when(F.col("order_status") == "RETURNED", 1)).alias("returned_count"),
        F.size(F.collect_set("order_status")).alias("num_order_status"),
    )
    .filter(F.col("distinct_ids") > 1)
    .withColumn("num_order_status_rank", F.rank().over(w4))
)
