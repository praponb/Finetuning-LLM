from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("event_type").orderBy("duration_sec_adjusted", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.round((F.col("duration_sec") * 1.15), 2).alias("duration_sec_adjusted"),
        F.coalesce("page", F.lit("Unknown")).alias("page_filled"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled_2"),
    )
)

result = (
    base
    .filter(F.col("duration_sec") < 137)
    .select(
        "id",
        "page",
        "page_filled",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.avg("duration_sec_adjusted").over(w_part).alias("duration_sec_adjusted_avg_5"),
        F.sum("duration_sec_adjusted").over(w3).alias("running_duration_sec_adjusted"),
    )
)
