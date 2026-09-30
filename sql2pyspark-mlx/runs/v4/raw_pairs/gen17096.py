from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") >= "2023-12-01")
    .select("id", "duration_sec", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("page").isin(["/home", "/cart", "/checkout"]))
        .select("id", "duration_sec", "event_type")
    )
)
