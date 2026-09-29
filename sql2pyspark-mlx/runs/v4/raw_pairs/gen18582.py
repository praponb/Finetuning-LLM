from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/checkout", "/cart"]))
    .select("id", "duration_sec", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") <= 173)
        .select("id", "duration_sec", "session_id")
    )
    .distinct()
)
