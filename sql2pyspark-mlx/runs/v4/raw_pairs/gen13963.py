from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.orderBy("is_new_session", "id").rowsBetween(-6, -1)
w5 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.coalesce("method", F.lit("Unknown")).alias("method_filled"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

flagged = (
    valid
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

scored = (
    sessions_payments
    .select(
        "*",
        F.row_number().over(w_ordered).alias("row_seq"),
        F.avg("is_new_session").over(w4).alias("is_new_session_avg_6_prior"),
        F.avg("is_new_session").over(w5).alias("is_new_session_avg_30d"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
        F.when(F.col("burst_id") >= 9, "HIGH").otherwise("LOW").alias("burst_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.count_if(F.col("method") == "promptpay").alias("promptpay_events"),
        F.min("paid_at").alias("first_seen"),
    )
    .filter(F.col("refunded_count") > 1)
    .orderBy(F.col("refunded_count").desc_nulls_last())
)
