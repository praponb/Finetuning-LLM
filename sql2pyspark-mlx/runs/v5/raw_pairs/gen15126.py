from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc())
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .select(
        "id",
        "page",
        "event_time",
        "event_type",
        "duration_sec",
        F.rank().over(w).alias("rnk"),
        F.row_number().over(w2).alias("rn"),
    )
)
