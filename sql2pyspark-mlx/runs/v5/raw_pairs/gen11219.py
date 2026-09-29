from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.page",
        "src.duration_sec",
        F.col("p.device"),
        F.col("p.country"),
        F.col("p.browser"),
    )
)
