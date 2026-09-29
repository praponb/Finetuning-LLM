from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter((F.col("s.country") == "UK") & ~F.col("we.page").isin(["/home", "/product"]))
    .select(
        F.col("we.event_time"),
        F.col("s.country"),
        F.when(F.col("we.event_type") == "add_to_cart", 1).otherwise(0).alias("is_add_to_cart"),
    )
    .orderBy("event_time")
)
