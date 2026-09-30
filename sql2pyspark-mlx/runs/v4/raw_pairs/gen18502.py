from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("started_at") >= F.to_timestamp(F.lit("2023-07-29 09:00:00")))
    .select(
        "*",
        F.length("device").alias("device_length"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

eligible = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

matched_keys = (
    eligible
    .select("id", "browser")
    .subtract(
        df_sessions
        .filter(F.col("country").isNotNull())
        .select("id", "browser")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_semi")
)

result = (
    kept
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .filter(F.col("chrome_events") < 1)
)
