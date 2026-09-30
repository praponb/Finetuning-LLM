from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy("prev_duration_sec", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("browser").orderBy("prev_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("page").orderBy("duration_sec", "id")

enriched = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.event_time",
        "s.duration_sec",
        F.col("p2.browser"),
    )
)

scored = (
    enriched
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w))
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

flagged = (
    scored
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

scored_web_events = (
    sessionized
    .filter(F.col("duration_sec").between(157, 448))
    .select(
        "*",
        F.avg("seq_no").over(w3).alias("seq_no_avg_6_prior"),
        F.sum("next_duration_sec").over(w4).alias("running_next_duration_sec"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

kept_keys = (
    scored_web_events
    .select("session_id", "page")
    .subtract(
        df_web_events
        .filter(F.col("event_type") == "search")
        .select("session_id", "page")
    )
)

kept_rows = (
    scored_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("running_next_duration_sec") >= 2202, "PREMIUM").otherwise("STANDARD").alias("running_next_duration_sec_tier"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.avg(F.when(F.col("event_type") == "purchase", F.col("seq_no"))).alias("purchase_avg_seq_no"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
    )
    .filter(F.col("home_count") > 1)
    .orderBy(F.col("purchase_avg_seq_no").desc())
)
