from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lower("page").alias("page_lower"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

result = (
    cleaned
    .withColumn("prev_duration_sec", F.lag("duration_sec", 1).over(w))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w2))
)
