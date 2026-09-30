from pyspark.sql import functions as F

joined = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.id", "e.session_id", "e.page", F.col("b.browser"))
)

base = (
    joined
    .filter(F.col("browser").isNotNull() & F.col("session_id").isin([12, 40]))
    .withColumn("page_prefix", F.substring("page", 1, 2))
)

enriched = (
    base
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.started_at") < "2023-12-12")
    .select("a.id", "a.session_id", "a.page", "a.browser", F.col("s.device"))
)

enriched_web_events = (
    enriched
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.device").alias("session_device"), F.col("p2.country"))
)

with_dims = (
    enriched_web_events
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.country").alias("session_country"))
)

result = (
    with_dims
    .groupBy("device")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.size(F.collect_set("session_country")).alias("num_session_countries"),
        F.count_if(F.col("page") == "/search").alias("search_events"),
    )
    .filter(F.col("search_events") >= 1)
    .orderBy(F.col("search_events").desc_nulls_first())
)
