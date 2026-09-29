from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
    )
    .orderBy(F.col("id").desc())
)
