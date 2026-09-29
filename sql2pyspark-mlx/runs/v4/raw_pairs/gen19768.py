from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy("duration_sec_scaled")
w2 = Window.partitionBy("browser")
w3 = Window.partitionBy("session_country").orderBy(F.col("duration_sec").desc_nulls_last())
w_recent = Window.orderBy(F.col("row_num_num").desc_nulls_last())

base = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2024-05-05").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.round((F.col("duration_sec") * 100), 2).alias("duration_sec_scaled"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

latest = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

base_web_events = (
    latest
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        "resolved_page",
        "row_num",
        F.least("duration_sec_scaled", "row_num").alias("least_duration_sec_scaled_row_num"),
        F.col("row_num").cast("double").alias("row_num_num"),
    )
)

with_dims = (
    base_web_events
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.browser"), F.col("d.country"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.country").alias("session_country"), F.col("p.device"))
)

matched_keys = (
    enriched
    .select("session_id", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("event_time") < "2025-03-01")
        .select("session_id", "event_type")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.sum("least_duration_sec_scaled_row_num").over(w2).alias("browser_sum_least_duration_sec_scaled_row_num"),
        F.cume_dist().over(w3).alias("duration_sec_cume_dist"),
        F.rank().over(w_recent).alias("row_num_num_rank"),
    )
)

result = (
    scored
    .withColumn("duration_sec_cume_dist_tier", F.when(F.col("duration_sec_cume_dist") >= 0.72, "PREMIUM").otherwise("STANDARD"))
    .withColumn("least_duration_sec_scaled_row_num_tier", F.when(F.col("least_duration_sec_scaled_row_num") >= 89, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["device", "duration_sec_cume_dist_tier", "least_duration_sec_scaled_row_num_tier"], ["device", "duration_sec_cume_dist_tier"], ["device"], []],
        "device",
        "duration_sec_cume_dist_tier",
        "least_duration_sec_scaled_row_num_tier",
    )
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.count("*").alias("row_count"),
        F.grouping("device").alias("grouping_level"),
    )
)
