from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-08-12"))) & F.col("page").isNotNull())
    .select(
        "*",
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

filtered = (
    islands
    .filter(F.col("event_time") >= "2023-06-18")
    .select(
        "id",
        "event_type",
        "duration_sec",
        "event_type_prefix",
        "is_new_burst",
        "session_no",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
    )
)

base_web_events = (
    filtered
    .filter(F.col("duration_sec") >= 173)
    .select(
        "id",
        "duration_sec",
        "event_type_prefix",
        "is_new_burst",
        "page_event_type_key",
        F.when(F.col("session_no") >= 7, "High").when(F.col("session_no") >= 1, "Medium").otherwise("Low").alias("session_no_band"),
    )
)

result = (
    base_web_events
    .select(
        "id",
        "duration_sec",
        "event_type_prefix",
        "is_new_burst",
        "page_event_type_key",
        "session_no_band",
    )
)
