from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-08-24").cast("date")) & (F.col("page") == "/search"))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.round("duration_sec", 0).alias("duration_sec_rounded"),
    )
)

result = (
    valid
    .groupBy("session_id")
    .pivot("page", ["/search", "/home", "/checkout"])
    .agg(F.sum("duration_sec_rounded").alias("total"))
    .select(
        "session_id",
        F.col("/search").alias("search"),
        F.col("/home").alias("home"),
        F.col("/checkout").alias("checkout"),
    )
)
