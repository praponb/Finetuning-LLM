from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        F.row_number().over(w).alias("event_time_row"),
    )
)
