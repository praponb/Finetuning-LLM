from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("browser").orderBy("started_at", "id")
w5 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

kept = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_semi")
)

flagged = (
    kept
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

with_last = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"))
)

gaps = (
    with_last
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w4).isNull() | (F.col("started_at") > F.lag("started_at").over(w4) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w5))

kept_keys = (
    islands
    .select("customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("browser").isin(["Firefox", "Safari"]))
        .select("customer_id")
    )
)

verified = (
    islands
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("browser")
    .exceptAll(
        df_sessions
        .filter(F.hour("started_at").between(9, 12))
        .select("browser")
    )
)

verified_sessions = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

with_dims = (
    verified_sessions
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.segment"), F.col("d.signup_date"), F.col("d.city"))
)

summary = (
    with_dims
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "LARGE").otherwise("SMALL").alias("session_start_flag_tier"),
        F.date_trunc("year", "started_at").alias("year_start"),
        "event_type",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.min("started_at").alias("first_started_at"),
    )
    .filter(F.col("event_count") > 1)
)

result = (
    summary
    .select(
        "session_start_flag_tier",
        "year_start",
        "event_type",
        "event_count",
        "first_started_at",
    )
)
