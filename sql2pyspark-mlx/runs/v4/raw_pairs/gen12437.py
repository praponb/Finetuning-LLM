from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 407, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.min("event_time").alias("first_seen"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
    .filter(F.col("duration_sec_bit_and") >= 1)
    .orderBy("duration_sec_bit_and")
)
