from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time")
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        "event_type",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.rank().over(w2).alias("event_time_rank"),
    )
)
