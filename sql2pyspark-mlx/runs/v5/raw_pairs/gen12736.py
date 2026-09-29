from pyspark.sql import functions as F

enriched = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .filter(~F.col("b.device").isin(["desktop", "mobile"]))
    .select("src.*", F.col("b.browser"))
)

result = (
    enriched
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.count_if(F.col("event_type") == "search").alias("search_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("duration_sec_per_add_to_cart"),
    )
    .orderBy(F.col("search_events").desc())
)
