from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("page", "event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec") != 365)
        .select("page", "event_type")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type").rlike("^(search|purchase)$"), "GROUP_A").when(F.col("event_type").rlike("^(page_view|click)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("event_type_zone"),
    )
)
