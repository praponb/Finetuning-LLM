from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page")
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w4 = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc(), "id")

scored = (
    df_web_events
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("cumulative_duration_sec"),
        F.max("duration_sec").over(w2).alias("page_max_duration_sec"),
        F.lead("duration_sec", 1).over(w3).alias("next_duration_sec"),
        F.ntile(5).over(w4).alias("duration_sec_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_type",
        "page",
        "cumulative_duration_sec",
        "duration_sec_bucket",
    )
)
