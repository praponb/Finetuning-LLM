from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time")
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.percent_rank().over(w).alias("event_time_pct_rank"),
        F.rank().over(w2).alias("rnk"),
    )
)
