from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .pivot("page", ["/product", "/search"])
    .agg(F.max("duration_sec"))
    .select(
        "event_type",
        F.col("/product").alias("product"),
        F.col("/search").alias("search"),
    )
)
