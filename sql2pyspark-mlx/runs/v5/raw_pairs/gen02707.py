from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id")
w3 = Window.partitionBy("device").orderBy(F.col("event_seq").asc_nulls_first())
w_recent = Window.partitionBy("country", "device").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("started_at", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("page").orderBy("web_event_event_time", "id")
w8 = Window.partitionBy("page").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("browser", "page").orderBy(F.col("running_sum_event_seq").desc(), "id")

metrics = (
    df_sessions
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    deduped
    .filter(F.col("device") == "tablet")
    .withColumn("running_sum_event_seq", F.sum("event_seq").over(w_recent))
    .withColumn("running_max_event_seq", F.max("event_seq").over(w_recent))
    .withColumn("running_count_event_seq", F.count("event_seq").over(w_recent))
    .withColumn("running_min_event_seq", F.min("event_seq").over(w_recent))
    .withColumn("event_seq_avg_6_prior", F.avg("event_seq").over(w5))
)

with_dims = (
    scored
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("a.*", F.col("b.segment"), F.col("b.email"), F.col("b.name"))
)

with_latest = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.event_seq",
        "s.running_sum_event_seq",
        "s.running_count_event_seq",
        F.col("c.page"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

with_gaps = (
    with_latest
    .withColumn("is_new_burst", F.when(F.lag("web_event_event_time").over(w7).isNull() | (F.col("web_event_event_time") > F.lag("web_event_event_time").over(w7) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w8))

latest = (
    sessions_sessions
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "running_sum_event_seq",
        "running_count_event_seq",
        "page",
        "is_new_burst",
    )
)

result = (
    latest
    .groupBy("page")
    .agg(
        F.avg(F.when(F.col("page") == "/checkout", F.col("running_count_event_seq"))).alias("checkout_avg_running_count_event_seq"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_sum_event_seq"), 2).alias("avg_running_sum_event_seq"),
    )
)
