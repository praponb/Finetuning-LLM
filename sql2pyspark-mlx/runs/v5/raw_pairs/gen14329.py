from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(331, 336))
    .select("session_id")
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([26, 36, 40]))
        .select("session_id")
    )
    .distinct()
    .orderBy(F.col("session_id").desc())
)
