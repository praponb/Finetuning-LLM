from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount_scaled").desc(), "id")
w_running = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("method").orderBy(F.col("amount_scaled_avg_6").asc_nulls_last())
w6 = Window.partitionBy("status").orderBy("paid_at", "id")
w7 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

base = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED"]))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.floor("amount").alias("amount_whole"),
        F.round((F.col("amount") * 1.1), 2).alias("amount_scaled"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

scored = (
    base
    .withColumn("amount_scaled_bucket", F.ntile(5).over(w))
    .withColumn("amount_scaled_avg_90d", F.avg("amount_scaled").over(w_running))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
    .withColumn("amount_scaled_avg_6", F.avg("amount_scaled").over(w4))
)

deduped = (
    scored
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

base_payments = (
    deduped
    .filter((F.col("paid_at") >= F.lit("2023-11-07").cast("date")) & F.col("amount_scaled_avg_6").between(35, 41))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "amount_whole",
        "method_filled",
        "amount_scaled_bucket",
        "amount_scaled_avg_90d",
        "amount_share",
        "amount_scaled_avg_6",
        "latest_rank",
        F.when(F.col("amount_scaled_avg_90d") >= 79, "High").when(F.col("amount_scaled_avg_90d") >= 35, "Medium").otherwise("Low").alias("amount_scaled_avg_90d_tier"),
        (F.col("amount") * F.col("amount_whole")).alias("amount_times_amount_whole"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

prev_events = base_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w6))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_id", F.sum("is_new_session").over(w7))

enriched = (
    sessions_payments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.shipping_fee"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select(
        "src.*",
        F.col("d.shipping_fee").alias("order_shipping_fee"),
        F.col("d.channel"),
        F.col("d.discount"),
    )
)

result = (
    with_dims
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "amount_whole",
        "amount_scaled_bucket",
        "amount_scaled_avg_6",
        "resolved_method",
        "is_new_session",
        F.when(F.col("method").rlike("^(promptpay|paypal)$"), "EXTENDED").otherwise("GROUP_B").alias("method_zone"),
        F.col("avg_payment_amount"),
    )
    .orderBy(F.col("is_new_session").asc_nulls_first())
)
