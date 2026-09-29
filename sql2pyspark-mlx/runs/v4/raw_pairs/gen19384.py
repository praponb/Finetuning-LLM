from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("browser").isin(["Safari", "Edge"])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(~F.col("we.event_type").isin(["search", "add_to_cart", "page_view"]))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.session_id"),
        F.col("we.event_time"),
    )
    .orderBy(F.col("page").desc())
)
