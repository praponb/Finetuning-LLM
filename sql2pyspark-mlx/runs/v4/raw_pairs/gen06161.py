from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "session_id",
        F.when(F.col("duration_sec") >= 440, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.count("*").alias("cnt"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
    .filter(F.col("duration_sec_bit_or") >= 1)
)
