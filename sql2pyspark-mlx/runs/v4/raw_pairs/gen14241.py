from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("page").isin(["/search", "/cart"]), "Tier 1").otherwise("Other").alias("page_group"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

result = (
    filtered
    .select("page")
    .subtract(
        df_web_events
        .filter(F.col("event_type").isin(["page_view", "purchase"]))
        .select("page")
    )
)
