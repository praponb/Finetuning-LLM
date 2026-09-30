from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("paid_at", "id")
w5 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

flagged = (
    ranked
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w3))

gaps = (
    sessionized
    .withColumn("is_new_session_2", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("is_new_session_2").over(w5))

result = (
    sessions_payments
    .groupBy(
        F.date_trunc("week", "paid_at").alias("week_start"),
        F.when(F.col("is_new_session_2") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_2_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("method") == "card", 1)), F.lit(0)), 2).alias("session_no_per_card"),
    )
    .orderBy(F.col("avg_is_new_session").desc_nulls_last())
)
