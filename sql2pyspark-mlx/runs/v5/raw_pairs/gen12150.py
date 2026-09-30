from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")

result = (
    df_web_events
    .select(
        "id",
        "page",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("page_sum_duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc())
)
