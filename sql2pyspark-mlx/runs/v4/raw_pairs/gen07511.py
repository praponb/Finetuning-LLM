from pyspark.sql import functions as F

with_dims = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.device"), F.col("p.country"), F.col("p.browser"))
)

result = (
    with_dims
    .withColumn("year_start", F.date_trunc("YEAR", "event_time"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "event_time"))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 538, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["year_start", "quarter_start", "duration_sec_tier"], ["year_start", "quarter_start"], ["year_start"], []],
        "year_start",
        "quarter_start",
        "duration_sec_tier",
    )
    .agg(
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.min("event_time").alias("first_seen"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/product", 1)), F.lit(0)), 2).alias("duration_sec_per_product"),
        F.grouping("year_start").alias("grouping_level"),
    )
    .filter(F.col("tablet_count") > 1)
)
