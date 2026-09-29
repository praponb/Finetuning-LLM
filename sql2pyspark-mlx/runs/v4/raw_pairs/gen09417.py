from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method", "status").orderBy(F.col("last_known_is_new_session").desc(), "id")
w_hist = Window.partitionBy("method").orderBy(F.col("last_known_is_new_session").desc_nulls_last())
w7 = Window.partitionBy("status").orderBy(F.col("last_known_is_new_session").desc())
w8 = Window.partitionBy("method", "order_id").orderBy("session_id", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("order_id").orderBy("session_id", "id")
w_recent = Window.orderBy("session_id", "id")

flagged = (
    df_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

valid = (
    sessions_payments
    .filter(F.col("method").isNotNull() & F.col("method").isin(["promptpay", "cash"]))
    .withColumn("method_group", F.when(F.col("method").isin(["cash", "promptpay"]), "Tier 1").otherwise("Other"))
)

flagged_2 = (
    valid
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = flagged_2.withColumn("session_id_2", F.sum("is_new_burst").over(w4))

metrics = (
    islands
    .withColumn("first_known_is_new_session", F.first("is_new_session", ignorenulls=True).over(w4))
    .withColumn("last_known_is_new_session", F.last("is_new_session", ignorenulls=True).over(w2))
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.dense_rank().over(w_hist).alias("last_known_is_new_session_dense_rank"),
        F.cume_dist().over(w7).alias("last_known_is_new_session_cume_dist"),
        F.avg("is_new_burst").over(w8).alias("is_new_burst_avg_5"),
        F.lag("session_id", 2).over(w9).alias("prev_session_id"),
        F.lag("last_known_is_new_session", 1).over(w_recent).alias("prev_last_known_is_new_session"),
    )
)

flagged_3 = (
    with_windows
    .withColumn("is_new_burst_2", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands_payments = flagged_3.withColumn("island_id", F.sum("is_new_burst_2").over(w2))

result = (
    islands_payments
    .groupBy("status", F.date_trunc("WEEK", "paid_at").alias("week_start"))
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.avg("first_known_is_new_session"), 2).alias("avg_first_known_is_new_session"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
    )
)
