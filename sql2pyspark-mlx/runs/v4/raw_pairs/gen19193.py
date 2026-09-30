from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/cart"]) & F.col("session_id").isin([1, 21, 36, 40]))
    .select(
        "session_id",
        "duration_sec",
        F.round((F.col("duration_sec") * 0.85), 2).alias("adjusted_duration_sec"),
    )
)
