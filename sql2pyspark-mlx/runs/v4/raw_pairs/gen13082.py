from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")
w2 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "page")
w4 = Window.partitionBy("session_id")
w5 = Window.partitionBy("browser").orderBy(F.col("event_time_date").desc(), "id")
w6 = Window.partitionBy("event_type").orderBy(F.col("event_seq").desc(), "id")

scored = (
    df_web_events
    .select(
        "*",
        F.min("duration_sec").over(w).alias("page_min_duration_sec"),
        F.first("page", ignorenulls=True).over(w2).alias("first_known_page"),
    )
)

base = (
    scored
    .filter((F.col("event_time") >= F.lit("2023-12-19").cast("date")) & F.col("page").isNotNull() & F.hour("event_time").between(12, 17))
    .withColumn("is_click", F.when(F.col("event_type") == "click", 1).otherwise(0))
    .withColumn("event_time_date", F.to_date("event_time"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("OTHER")))
)

enriched = (
    base
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.device"), F.col("s.browser"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.max("duration_sec").over(w3).alias("session_id_max_duration_sec"),
        F.avg("is_click").over(w4).alias("session_id_avg_is_click"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

filtered = (
    deduped
    .filter(F.col("event_type").isin(["click", "purchase", "page_view"]))
    .select(
        "*",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
        F.substring("event_type", 1, 4).alias("event_type_prefix"),
        F.coalesce("device", F.lit("DIRECT")).alias("device_clean"),
    )
)

result = (
    filtered
    .withColumn("page_min_duration_sec_tier", F.when(F.col("page_min_duration_sec") >= 365, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["page_min_duration_sec_tier", "browser"], ["page_min_duration_sec_tier"]],
        "page_min_duration_sec_tier",
        "browser",
    )
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("is_click")).otherwise(0)).alias("search_is_click"),
        F.round(F.sum("session_id_max_duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/home", 1)), F.lit(0)), 2).alias("session_id_max_duration_sec_per_home"),
        F.round(F.avg("session_id_avg_is_click"), 2).alias("avg_session_id_avg_is_click"),
        F.max("event_time").alias("last_event_time"),
    )
)
