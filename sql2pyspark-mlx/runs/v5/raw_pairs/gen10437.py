from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2023-07-01") & (F.col("duration_sec").isNotNull() | (F.col("duration_sec") < 398)))
    .withColumn("clean_page", F.trim("page"))
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
)
