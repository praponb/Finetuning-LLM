from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device", "country").orderBy(F.col("prev_started_at").desc_nulls_last(), "id")
w_running = Window.partitionBy("browser").orderBy(F.col("prev_started_at").desc(), "id", "subscription_id")
w5 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("started_at", "id", "subscription_id")
w7 = Window.partitionBy("browser").orderBy("monthly_fee", "id", "subscription_id")
w8 = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w9 = Window.partitionBy("customer_id")
w10 = Window.partitionBy("country").orderBy("row_seq", "id", "subscription_id")

valid = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .withColumn("clean_device", F.trim("device"))
)

prev_events = valid.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

deduped = (
    sessionized
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_window = (
    deduped
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.started_at") >= F.col("sub.start_date")) & (F.col("e.started_at") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

metrics = (
    with_window
    .select(
        "*",
        F.row_number().over(w_running).alias("row_seq"),
        F.last("subscription_id", ignorenulls=True).over(w5).alias("last_known_subscription_id"),
    )
)

scored = (
    metrics
    .withColumn("event_seq", F.row_number().over(w_recent))
    .withColumn("next_row_seq", F.lead("row_seq", 1, 0).over(w7))
    .withColumn("session_start_flag_avg_7d", F.avg("session_start_flag").over(w8))
    .withColumn("customer_id_avg_subscription_id", F.avg("subscription_id").over(w9))
    .withColumn("row_seq_bucket", F.ntile(3).over(w10))
)

result = (
    scored
    .groupBy("country", "customer_id")
    .pivot("device", ["desktop", "mobile"])
    .agg(F.avg("burst_id").alias("total"), F.count(F.lit(1)).alias("n"))
)
