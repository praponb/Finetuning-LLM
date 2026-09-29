from pyspark.sql import functions as F

joined = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.session_id",
        "a.event_time",
        "a.duration_sec",
        F.col("p.country"),
    )
)

enriched = (
    joined
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .filter(~F.col("d.country").isin(["JP", "DE"]))
    .select("e.*", F.col("d.device"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.country",
        F.col("b.country").alias("session_country"),
    )
)

with_dims_web_events = (
    with_dims
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.device"),
        F.col("b.country").alias("session_country_2"),
        F.col("b.browser"),
    )
)

result = (
    with_dims_web_events
    .groupBy("session_id", "browser")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.countDistinct("session_country_2").alias("distinct_session_country_2s"),
        F.size(F.collect_set("session_country")).alias("num_session_countries"),
        F.countDistinct("session_country_2").alias("distinct_session_country_2s_2"),
    )
)
