from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 11, 12, 35]))
    .select(
        "id",
        "page",
        F.when(F.col("page") == "/cart", 1).otherwise(0).alias("is_cart"),
        F.lower("event_type").alias("event_type_lower"),
    )
)
