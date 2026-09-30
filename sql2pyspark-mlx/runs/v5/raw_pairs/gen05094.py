from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.hour("started_at").alias("started_at_hour"),
        F.lower("device").alias("device_lower"),
    )
)

prev_events = valid.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

eligible = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.prev_started_at")), "left_semi")
)

matched_keys = (
    eligible
    .select("id", "customer_id")
    .exceptAll(
        df_sessions
        .filter(F.hour("started_at").between(8, 16))
        .select("id", "customer_id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL").alias("new_group_flag_tier"),
    )
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("browser") == "Firefox", 1)), F.lit(0)), 2).alias("session_no_per_firefox"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        F.sum(F.when(F.col("device") == "desktop", F.col("session_no")).otherwise(0)).alias("desktop_session_no"),
        F.count("*").alias("event_count"),
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
    )
    .filter(F.col("session_no_per_firefox") >= 0)
    .orderBy(F.col("new_group_flag_tier").desc_nulls_first())
)
