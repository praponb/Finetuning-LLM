from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("prev_paid_at", "id")
w4 = Window.partitionBy("status").orderBy("prev_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-06-03").cast("date"))
    .select(
        "*",
        F.round((F.col("amount") * 1.07), 2).alias("amount_scaled"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

cleaned_payments = (
    cleaned
    .filter(F.col("method").isNotNull())
    .withColumn("amount_scaled_filled", F.coalesce("amount_scaled", F.lit(0)))
    .withColumn("amount_scaled_adjusted", F.round((F.col("amount_scaled") * 1.15), 2))
)

prev_events = cleaned_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

flagged = (
    sessionized
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at") - F.lag("prev_paid_at").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w4))

grouped = (
    islands
    .groupBy(
        F.when(F.col("burst_id") >= 7, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count_if(F.col("method") == "card").alias("card_events"),
        F.max("prev_paid_at").alias("ended_at"),
        F.count("*").alias("event_count"),
    )
)

result = (
    grouped
    .select("burst_id_tier", "total_amount", "card_events")
    .orderBy(F.col("total_amount").desc(), F.col("card_events").desc())
)
