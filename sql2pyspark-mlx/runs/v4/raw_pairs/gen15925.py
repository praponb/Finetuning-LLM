from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2025-07-25") & (F.col("page") == "/product"))
    .select(
        "id",
        "duration_sec",
        "event_type",
        F.trim("event_type").alias("clean_event_type"),
        F.when(F.col("page").isin(["/cart", "/checkout"]), "Tier 1").otherwise("Other").alias("page_group"),
    )
    .orderBy("event_type")
)
