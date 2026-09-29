from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "browser").orderBy(F.col("started_at").asc_nulls_last(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("browser", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("started_at", "id")
w5 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("country").orderBy("duration_sec", "id")
w8 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.lower("browser").alias("browser_lower"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    latest
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w3))
    .withColumn("last_known_country_2", F.last("country", ignorenulls=True).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("last_known_country_3", F.last("country", ignorenulls=True).over(w5))
)

with_latest = (
    metrics
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"), F.col("c.duration_sec"))
)

scored = (
    with_latest
    .select(
        "*",
        F.lead("row_seq", 1).over(w4).alias("next_row_seq"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

flagged = (
    scored
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w4) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w8))

with_prev = sessions_sessions.withColumn("prev_started_at", F.lag("started_at").over(w4))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w8))

result = (
    sessionized
    .groupBy(
        F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.sum(F.when(F.col("browser") == "Chrome", F.col("seq_no")).otherwise(0)).alias("chrome_seq_no"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.size(F.collect_set("web_event_page")).alias("num_web_event_pages"),
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
    )
)
