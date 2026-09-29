from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "status").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("amount").desc_nulls_last())
w3 = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("channel")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

kept = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") >= F.col("s.order_date")), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "customer_id", "channel", "amount", "shipping_fee")
)

base = (
    deduped
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "shipping_fee",
        F.when(F.col("channel").isin(["mobile", "store"]), "Core").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

metrics = (
    base
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "channel_clean",
        F.dense_rank().over(w2).alias("amount_dense_rank"),
        F.avg("shipping_fee").over(w3).alias("shipping_fee_avg_3"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"), F.col("c.delivered_date"))
)

with_latest_orders = (
    with_latest
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.amount",
        "s.shipping_fee_avg_3",
        "s.amount_share",
        "s.cost",
        "s.delivered_date",
        F.col("c.delivered_date").alias("shipment_delivered_date"),
    )
)

totals = (
    with_latest_orders
    .groupBy(
        F.when(F.col("amount_share") >= 0.88, "PREMIUM").otherwise("STANDARD").alias("amount_share_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.max("amount").alias("max_amount"),
    )
)

result = totals.select("amount_share_tier", "event_count", "max_amount")
