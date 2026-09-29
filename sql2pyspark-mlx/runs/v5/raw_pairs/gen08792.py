from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-06-18 17:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .withColumn("started_at_quarter", F.quarter("started_at"))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("NONE")))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.to_date(F.col("c.event_time")) >= "2025-02-22"), "left_anti")
)

gaps = (
    not_flagged
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w2).isNull() | (F.col("started_at") > F.lag("started_at").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w3))

result = (
    sessionized
    .groupBy(
        "browser",
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        "customer_id",
    )
    .agg(
        F.avg(F.when(F.col("device") == "tablet", F.col("is_new_session"))).alias("tablet_avg_is_new_session"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.sum(F.when(F.col("country") == "SG", F.col("started_at_quarter"))).alias("sg_total_started_at_quarter"),
        F.count("*").alias("event_count"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
    )
    .filter(F.col("avg_is_new_session") > 0)
)
