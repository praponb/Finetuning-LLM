from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select(
        "id",
        "page",
        "event_time",
        "event_type",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
    .select("id", "page", "event_time", "event_type")
    .orderBy("event_time")
)
