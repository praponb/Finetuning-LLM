from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")

result = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .select(
        "id",
        "page",
        "duration_sec",
        F.avg("duration_sec").over(w).alias("page_avg_duration_sec"),
    )
)
