from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status")
w2 = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").desc_nulls_first())

enriched = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.delivered_date",
        "a.status",
        F.col("d.shipping_fee"),
    )
)

remaining_keys = (
    enriched
    .select("status", "carrier")
    .exceptAll(
        df_shipments
        .filter(F.col("shipped_date") > "2025-04-01")
        .select("status", "carrier")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

enriched_shipments = (
    reconciled
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount").between(430, 870))
    .select(
        "a.order_id",
        "a.carrier",
        "a.delivered_date",
        "a.shipping_fee",
        F.col("o.status").alias("order_status"),
        F.col("o.channel"),
    )
)

enriched_shipments_2 = (
    enriched_shipments
    .select(
        "order_id",
        "order_status",
        "channel",
        F.min("shipping_fee").over(w).alias("order_status_min_shipping_fee"),
        F.cume_dist().over(w2).alias("shipping_fee_cume_dist"),
    )
)

result = (
    enriched_shipments_2
    .select("order_status", "order_status_min_shipping_fee", "shipping_fee_cume_dist")
    .orderBy(
        F.col("order_status_min_shipping_fee").asc_nulls_first(),
        "shipping_fee_cume_dist",
    )
)
