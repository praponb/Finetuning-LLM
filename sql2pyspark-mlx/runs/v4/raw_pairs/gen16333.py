from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 384, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["duration_sec_tier", "page"], ["duration_sec_tier"], []],
        "duration_sec_tier",
        "page",
    )
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
    )
    .orderBy(F.col("duration_sec_bit_or").desc())
)
