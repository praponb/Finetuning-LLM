from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("page").isin(["/home", "/search", "/checkout"]))
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
    .withColumn("event_type_length", F.length("event_type"))
    .withColumn("page_clean", F.coalesce("page", F.lit("N/A")))
)

result = (
    base
    .select("id")
    .intersectAll(
        df_web_events
        .filter(F.col("event_time") >= "2025-09-01")
        .select("id")
    )
)
