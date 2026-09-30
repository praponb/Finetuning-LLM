from pyspark.sql import functions as F

joined = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.browser"), F.col("b.country"), F.col("b.device"))
)

result = (
    joined
    .rollup(
        F.date_trunc("YEAR", "event_time").alias("year_start"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("duration_sec_per_desktop"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles_2"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
    )
)
