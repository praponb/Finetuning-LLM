from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 32, 36]))
    .select("page", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") >= 402)
        .select("page", "duration_sec")
    )
)
