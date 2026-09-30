from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-10-15").cast("date")) & (F.col("event_type") != "page_view"))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("page") == "/home", 1).otherwise(0).alias("is_home"),
    )
)

result = (
    base
    .select("page")
    .subtract(
        df_web_events
        .filter(F.col("event_time") < "2023-04-01")
        .select("page")
    )
)
