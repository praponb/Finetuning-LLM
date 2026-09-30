from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page", "session_id")
    .intersect(
        df_web_events
        .filter(F.col("page") == "/product")
        .select("page", "session_id")
    )
)
