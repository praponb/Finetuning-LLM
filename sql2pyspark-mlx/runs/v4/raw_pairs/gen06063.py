from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("paid_at_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("channel", "order_id")
w5 = Window.partitionBy("status")
w6 = Window.partitionBy("status").orderBy(F.col("paid_at_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("order_id", "status").orderBy("session_id", "id")
w8 = Window.partitionBy("channel").orderBy("paid_at_period", "id")
w9 = Window.partitionBy("channel").orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("method").orderBy(F.col("prev_paid_at_period").asc_nulls_last(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-12-18 06:00:00")))
    .select("*", F.date_trunc("month", "paid_at").alias("paid_at_period"))
)

flagged = (
    filtered
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

enriched = (
    sessionized
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.shipping_fee"), F.col("d.order_date"), F.col("d.channel"))
)

filtered_payments = (
    enriched
    .filter(F.hour("paid_at").between(7, 11))
    .select("*", F.month("order_date").alias("order_date_month"))
)

enriched_payments = (
    filtered_payments
    .select(
        "*",
        F.avg("is_new_session").over(w3).alias("is_new_session_avg_30d"),
        F.avg("order_date_month").over(w4).alias("channel_avg_order_date_month"),
        F.min("shipping_fee").over(w5).alias("status_min_shipping_fee"),
        F.sum("is_new_session").over(w6).alias("is_new_session_sum_30d"),
        F.lag("session_id", 1, 0).over(w7).alias("prev_session_id"),
    )
)

with_prev = (
    enriched_payments
    .withColumn("prev_paid_at_period", F.lag("paid_at_period").over(w8))
)

gaps = (
    with_prev
    .withColumn("is_new_session_2", F.when(F.col("prev_paid_at_period").isNull() | (F.col("paid_at_period") > F.col("prev_paid_at_period") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized_payments = gaps.withColumn("session_id_2", F.sum("is_new_session_2").over(w9))

ranked = (
    sessionized_payments
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "is_new_session",
        "session_id",
        "shipping_fee",
        F.col("max_payment_amount"),
    )
)
