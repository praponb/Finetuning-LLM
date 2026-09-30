from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time")
w2 = Window.partitionBy("page").orderBy(F.col("event_type_length").desc_nulls_first(), "id")

ranked = (
    df_web_events
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "session_id", "event_type", "page")
)

cleaned = (
    ranked
    .filter(F.col("page").isNotNull())
    .select("*", F.lower("event_type").alias("event_type_lower"))
)

base = (
    cleaned
    .filter(F.col("event_type") == "page_view")
    .withColumn("event_type_length", F.length("event_type"))
    .withColumn("page_filled", F.coalesce("page", F.lit("OTHER")))
)

cleaned_web_events = (
    base
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/search"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_type_lower",
        "event_type_length",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
    )
)

joined = (
    cleaned_web_events
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.event_type_lower",
        "s.event_type_length",
        F.col("d.device"),
        F.col("d.country"),
    )
)

deduped = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

joined_web_events = (
    deduped
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select(
        "src.*",
        F.col("s.device").alias("session_device"),
        F.col("s.country").alias("session_country"),
    )
)

result = (
    joined_web_events
    .select(
        "id",
        "page",
        "event_type_lower",
        "event_type_length",
        "country",
        "rank_in_group",
        "session_device",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
