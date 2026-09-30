from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

filtered = (
    latest
    .filter(F.col("row_num") != 1)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lower("page").alias("page_lower"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "page_clean",
        F.sum("new_group_flag").over(w3).alias("session_no"),
    )
)

with_dims = (
    sessionized
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.event_time",
        "src.page_clean",
        F.col("p.country"),
        F.col("p.browser"),
        F.col("p.device"),
    )
)

flagged_2 = (
    with_dims
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged_2.withColumn("island_id", F.sum("session_start_flag").over(w3))

valid = (
    sessions_web_events
    .filter(F.col("country").isin(["UK", "SG", "US"]))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "page_clean",
        "country",
        "browser",
        "device",
        "session_start_flag",
        "island_id",
        F.upper("page").alias("page_upper"),
        F.length("page").alias("page_length"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

summary = (
    valid
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["country", "session_start_flag_tier", "session_id"], ["country", "session_start_flag_tier"], ["country"]],
        "country",
        "session_start_flag_tier",
        "session_id",
    )
    .agg(
        F.sum(F.when(F.col("device") == "desktop", F.col("session_start_flag")).otherwise(0)).alias("desktop_session_start_flag"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.grouping("country").alias("gid"),
    )
    .filter(F.col("desktop_session_start_flag") >= 0)
)

result = (
    summary
    .select(
        "country",
        "session_start_flag_tier",
        "session_id",
        "desktop_session_start_flag",
        "span_minutes",
        "gid",
    )
)
