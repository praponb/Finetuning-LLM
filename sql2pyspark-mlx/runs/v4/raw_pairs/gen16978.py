from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2024-02-23 12:00:00").cast("timestamp")) & (F.col("browser") == "Edge"))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

remaining_keys = (
    cleaned
    .select("device", "id")
    .intersect(
        df_sessions
        .filter(F.to_date("started_at") >= "2023-04-01")
        .select("device", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

grouped = (
    eligible
    .groupBy(
        F.date_trunc("QUARTER", "started_at_day").alias("quarter_start"),
        F.date_trunc("YEAR", "started_at").alias("year_start"),
        "country",
    )
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.countDistinct("country").alias("distinct_countries"),
    )
)

result = (
    grouped
    .select(
        "quarter_start",
        "year_start",
        "country",
        "th_count",
        "distinct_browsers",
        "distinct_countries",
    )
)
