from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-10-05 04:00:00").cast("timestamp")) & (F.col("method") == "promptpay"))
    .withColumn("status_length", F.length("status"))
    .withColumn("amount_rounded", F.round("amount", 1))
)

gaps = (
    base
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status_length",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

enriched = (
    islands
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.shipping_fee"), F.col("o.status"))
)

enriched_payments = (
    enriched
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.session_no",
        F.col("o.shipping_fee").alias("order_shipping_fee"),
        F.col("o.order_date"),
    )
)

kept_keys = (
    enriched_payments
    .select("method", "order_id")
    .exceptAll(
        df_payments
        .filter(F.col("amount") <= 810)
        .select("method", "order_id")
    )
)

kept_rows = (
    enriched_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.channel").alias("channel"), F.col("e.method").alias("method"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.session_no")).alias("max_session_no"),
        F.min(F.col("e.order_shipping_fee")).alias("min_order_shipping_fee"),
    )
    .orderBy(F.col("max_session_no").asc_nulls_last())
)
