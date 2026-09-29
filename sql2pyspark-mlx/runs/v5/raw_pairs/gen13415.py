from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") != "search")
    .select("page")
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([1, 19, 28]))
        .select("page")
    )
    .distinct()
    .orderBy(F.col("page").desc())
)
