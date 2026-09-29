from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.device").isin(["tablet", "desktop"]))
    .select(
        "e.id",
        "e.session_id",
        F.col("s.country"),
        F.col("s.browser"),
        F.col("s.device"),
    )
)
