from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
)

result = (
    base
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") >= 270)
        .select("id")
    )
)
