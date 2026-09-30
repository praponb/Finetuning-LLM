from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("method").isNotNull() & ~F.col("method").isin(["bank_transfer", "card", "paypal"]))
    .withColumn("paid_at_day", F.col("paid_at").cast("date"))
)

prev_events = filtered.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 1720, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.countDistinct("status").alias("distinct_status"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("distinct_status").desc_nulls_first())
)
