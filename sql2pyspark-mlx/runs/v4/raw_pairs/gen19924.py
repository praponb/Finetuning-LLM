from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("amount")

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

prev_events_2 = islands.withColumn("prev_paid_at_2", F.lag("paid_at").over(w3))

flagged = (
    prev_events_2
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at_2").isNull() | (F.col("paid_at") > F.col("prev_paid_at_2") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands_payments = flagged.withColumn("session_id", F.sum("new_group_flag").over(w4))

ranked = (
    islands_payments
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    ranked
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.order_date"), F.col("d.channel"))
)

daily = (
    joined
    .withColumn("new_group_flag_tier", F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "prev_paid_at"))
    .withColumn("activity_month", F.date_trunc("MONTH", "prev_paid_at_2"))
    .groupingSets(
        [["new_group_flag_tier", "week_start", "activity_month"], ["new_group_flag_tier", "week_start"], ["new_group_flag_tier"], []],
        "new_group_flag_tier",
        "week_start",
        "activity_month",
    )
    .agg(
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.grouping("new_group_flag_tier").alias("gid"),
    )
    .filter(F.col("island_id_p95") >= 0)
)

result = (
    daily
    .select("new_group_flag_tier", "week_start", "island_id_p95", "avg_session_id")
    .orderBy(F.col("island_id_p95").desc())
)
