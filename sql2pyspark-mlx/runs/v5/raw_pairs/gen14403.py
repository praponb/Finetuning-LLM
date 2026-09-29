from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("browser").orderBy("burst_id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w_grp = Window.partitionBy("browser", "customer_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id", "browser").orderBy("burst_id_cume_dist", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("browser")
w_ordered = Window.partitionBy("customer_id").orderBy("burst_id_cume_dist")

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    flagged
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

with_windows = (
    sessions_sessions
    .filter(F.col("browser").isin(["Firefox", "Chrome", "Edge"]))
    .select(
        "id",
        "customer_id",
        "browser",
        F.cume_dist().over(w_part).alias("burst_id_cume_dist"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_windows_sessions = (
    with_windows
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w_grp).alias("last_known_browser"),
        F.avg("event_seq").over(w6).alias("event_seq_avg_5_prior"),
    )
)

matched_keys = (
    with_windows_sessions
    .select("id")
    .intersectAll(
        df_sessions
        .filter(F.col("browser").isin(["Chrome", "Safari"]))
        .select("id")
    )
)

reconciled = (
    with_windows_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("browser_stddev_samp_event_seq", F.stddev_samp("event_seq").over(w7))
    .withColumn("burst_id_cume_dist_cume_dist", F.cume_dist().over(w_ordered))
)

joined = (
    scored
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.last_known_browser",
        "s.event_seq_avg_5_prior",
        "s.browser_stddev_samp_event_seq",
        "s.burst_id_cume_dist_cume_dist",
        F.col("c.name"),
    )
)

result = (
    joined
    .select(
        "id",
        "last_known_browser",
        "browser_stddev_samp_event_seq",
        "burst_id_cume_dist_cume_dist",
    )
)
