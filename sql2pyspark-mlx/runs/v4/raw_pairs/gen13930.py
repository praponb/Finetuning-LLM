from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .select("id", "page", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("event_type") == "purchase")
        .select("id", "page", "session_id")
    )
    .distinct()
    .orderBy("id")
)
