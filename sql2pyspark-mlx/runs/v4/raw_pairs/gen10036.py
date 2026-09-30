from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select("session_id", "duration_sec")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([14, 15, 26, 27]))
        .select("session_id", "duration_sec")
    )
)
