from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec").asc_nulls_last())
w_hist = Window.partitionBy("event_type")

base = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2024-05-11 05:00:00"))) & F.col("duration_sec").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.trim("page").alias("clean_page"),
    )
)

gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

enriched = (
    sessionized
    .withColumn("duration_sec_bucket", F.ntile(4).over(w3))
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w4))
    .withColumn("event_type_min_island_id", F.min("island_id").over(w_hist))
)

cleaned = (
    enriched
    .filter((F.col("event_time") >= F.lit("2023-06-22 16:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "clean_page",
        "island_id",
        "duration_sec_bucket",
        "duration_sec_cume_dist",
        "event_type_min_island_id",
        F.ceil("island_id").alias("island_id_whole"),
    )
)

kept_keys = (
    cleaned
    .select("id")
    .exceptAll(
        df_web_events
        .filter(~F.col("event_type").isin(["add_to_cart", "click"]))
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned_web_events = (
    reconciled
    .filter(F.col("event_type_min_island_id").between(2, 6))
    .select(
        "*",
        F.concat("page", F.lit("/"), "event_type").alias("page_event_type"),
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
    )
)

result = (
    cleaned_web_events
    .select(
        "id",
        "page",
        "event_time",
        "clean_page",
        "island_id",
        "duration_sec_bucket",
        "island_id_whole",
        "event_time_label",
    )
    .orderBy(F.col("event_time").desc_nulls_last())
)
