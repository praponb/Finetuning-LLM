from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("carrier").orderBy("shipping_fee", "id")
w_recent = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier", "order_id").orderBy("shipping_fee")
w4 = Window.orderBy(F.col("order_shipping_fee").desc_nulls_first())
w5 = Window.partitionBy("channel").orderBy("delivered_date", "id")
w6 = Window.partitionBy("status").orderBy(F.col("cumulative_shipping_fee").desc(), "id")
w7 = Window.partitionBy("channel", "order_id")
w8 = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("carrier")

filtered = (
    df_shipments
    .filter(F.col("delivered_date") > F.to_date(F.lit("2024-01-11")))
    .select(
        "*",
        F.when(F.col("carrier").isin(["Flash", "FedEx"]), "Tier 1").otherwise("Other").alias("carrier_group"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.shipping_fee"), F.col("o.discount"), F.col("o.channel"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.shipping_fee").alias("order_shipping_fee"))
)

scored = (
    with_dims
    .select(
        "*",
        F.lag("order_shipping_fee", 2).over(w_ordered).alias("prev_order_shipping_fee"),
        F.sum("shipping_fee").over(w_recent).alias("cumulative_shipping_fee"),
    )
)

metrics = (
    scored
    .withColumn("shipping_fee_dense_rank", F.dense_rank().over(w3))
    .withColumn("order_shipping_fee_rank", F.rank().over(w4))
)

enriched = (
    metrics
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("cumulative_shipping_fee_bucket", F.ntile(10).over(w6))
)

result = (
    enriched
    .withColumn("channel_max_order_shipping_fee", F.max("order_shipping_fee").over(w7))
    .withColumn("cumulative_shipping_fee_avg_3_prior", F.avg("cumulative_shipping_fee").over(w8))
    .withColumn("carrier_count_cost", F.count("cost").over(w9))
)
