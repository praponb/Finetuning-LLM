from pyspark.sql import functions as F

joined = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.country").isin(["JP", "TH"]))
    .select("a.*", F.col("b.browser"), F.col("b.device"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("duration_sec") >= 403, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.max(F.when(F.col("event_type") == "purchase", F.col("duration_sec"))).alias("purchase_max_duration_sec"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
