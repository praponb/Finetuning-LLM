from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") >= "2023-05-01") & F.col("page").isNotNull())
    .select(
        "*",
        F.lower("page").alias("page_lower"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        F.sum("is_new_session").over(w2).alias("session_id_2"),
    )
)

result = (
    sessions_web_events
    .cube("event_type")
    .agg(
        F.percentile_approx("session_id_2", [0.25, 0.5, 0.75]).alias("session_id_2_quartiles"),
        F.round(F.sum("session_id_2") / F.nullif(F.count(F.when(F.col("event_type") == "click", 1)), F.lit(0)), 2).alias("session_id_2_per_click"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.grouping_id().alias("gid"),
    )
)
