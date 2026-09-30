from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        "event_time",
        F.length("page").alias("page_length"),
        F.when(F.col("page").isin(["/cart", "/home"]), "Tier 1").otherwise("Other").alias("page_group"),
    )
    .orderBy(F.col("event_time").desc(), "id")
    .limit(5)
)
