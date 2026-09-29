from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id").orderBy("amount_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("amount_rounded").desc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("paid_at", "id")
w5 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-08-12").cast("date")) & (F.col("order_id") == 16))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.round("amount", 2).alias("amount_rounded"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

enriched = (
    filtered
    .filter(F.to_date("paid_at") >= "2024-12-28")
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w_running))
    .withColumn("amount_rounded_dense_rank", F.dense_rank().over(w2))
    .withColumn("last_known_amount_2", F.last("amount", ignorenulls=True).over(w3))
)

enriched_payments = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("src.*", F.col("p.shipping_fee"), F.col("p.status").alias("order_status"))
)

enriched_payments_2 = (
    enriched_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.amount",
        "s.paid_at",
        "s.status",
        "s.amount_rounded",
        "s.last_known_amount",
        "s.last_known_amount_2",
        "s.shipping_fee",
        F.col("o.channel"),
        F.col("o.shipping_fee").alias("order_shipping_fee"),
    )
)

with_gaps = (
    enriched_payments_2
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w4).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w4) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w5))

with_dims = (
    sessions_payments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "src.*",
        F.col("d.discount"),
        F.col("d.shipping_fee").alias("order_shipping_fee_2"),
        F.col("d.status").alias("order_status"),
    )
)

with_dims_payments = (
    with_dims
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.paid_at",
        "src.status",
        "src.shipping_fee",
        "src.order_shipping_fee",
        "src.discount",
        "src.order_shipping_fee_2",
        F.col("b.shipping_fee").alias("order_shipping_fee_3"),
    )
)

result = (
    with_dims_payments
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "discount",
        "order_shipping_fee_2",
        F.col("sum_payment_amount"),
    )
)
