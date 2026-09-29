from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.last_day("delivered_date").alias("delivered_date_month_end"))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.cost",
        "src.delivered_date_month_end",
        F.col("p.discount"),
        F.col("p.status").alias("order_status"),
    )
)

enriched_shipments = (
    enriched
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.cost",
        "s.discount",
        "s.order_status",
        F.col("b.shipping_fee"),
        F.col("b.discount").alias("order_discount"),
    )
)

with_dims = (
    enriched_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.cost",
        "e.order_status",
        "e.order_discount",
        F.col("d.amount"),
        F.col("d.channel"),
    )
)

result = (
    with_dims
    .select("id", "cost", "order_status", "order_discount", "channel")
    .orderBy("id", "order_status")
)
