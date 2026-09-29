from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([23, 25, 28]))
    .groupBy("session_id")
    .pivot("page", ["/home", "/search", "/checkout", "/cart"])
    .agg(F.count("duration_sec"))
    .orderBy("session_id")
)
