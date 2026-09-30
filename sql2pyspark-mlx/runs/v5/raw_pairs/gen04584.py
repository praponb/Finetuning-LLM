from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(11, 19) | (F.col("page") == "/product"))
    .select(
        "duration_sec",
        "page",
        "session_id",
        F.initcap(F.trim("page")).alias("page_title"),
    )
    .orderBy("page_title")
)
