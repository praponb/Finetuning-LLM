from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "page").orderBy("duration_sec")
w4 = Window.partitionBy("session_id")

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

enriched = (
    cleaned
    .withColumn("last_known_page", F.last("page", ignorenulls=True).over(w_hist))
    .withColumn("first_known_page", F.first("page", ignorenulls=True).over(w_part))
)

remaining_keys = (
    enriched
    .select("id", "page")
    .subtract(
        df_web_events
        .filter(F.hour("event_time").between(6, 10))
        .select("id", "page")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

enriched_web_events = (
    kept_rows
    .withColumn("duration_sec_dense_rank", F.dense_rank().over(w3))
    .withColumn("session_id_count_duration_sec", F.count("duration_sec").over(w4))
)

result = (
    enriched_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "event_type_prefix",
        "page_clean",
        "last_known_page",
    )
)
