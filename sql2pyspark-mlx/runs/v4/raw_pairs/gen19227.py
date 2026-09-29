from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "customer_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device").orderBy("session_start_flag", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("browser").orderBy("session_start_flag", "id")
w7 = Window.partitionBy("device").orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device", "country").orderBy(F.col("island_id").desc(), "id")

ranked = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

gaps = (
    ranked
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("session_start_flag").over(w3))

with_windows = (
    islands
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w4).alias("last_known_browser"),
        F.avg("island_id").over(w5).alias("island_id_avg_6"),
        F.ntile(4).over(w6).alias("session_start_flag_bucket"),
        F.sum("session_start_flag").over(w7).alias("running_session_start_flag"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

with_window = (
    with_windows
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .groupBy(
        F.date_trunc("YEAR", "started_at").alias("year_start"),
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
        F.date_trunc("WEEK", "started_at").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.countDistinct("device").alias("distinct_devices"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.percentile_approx("island_id_avg_6", 0.95).alias("island_id_avg_6_p95"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
    )
)
