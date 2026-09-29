from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([5, 8, 9, 13]))
    .select("id", "session_id", "page")
    .unionByName(
        df_web_events
        .filter(F.col("event_type") == "add_to_cart")
        .select("id", "session_id", "page")
    )
    .distinct()
    .orderBy("page")
)
