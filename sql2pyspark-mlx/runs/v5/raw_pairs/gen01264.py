from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("duration_sec", "id")

scored = (
    df_web_events
    .filter(F.col("session_id").isin([18, 24, 30]))
    .select(
        "id",
        "session_id",
        F.first("page", ignorenulls=True).over(w).alias("first_known_page"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
        F.lead("duration_sec", 2).over(w3).alias("next_duration_sec"),
    )
)

result = (
    scored
    .groupBy("session_id")
    .agg(
        F.min("next_duration_sec").alias("min_next_duration_sec"),
        F.percentile_approx("next_duration_sec", 0.5).alias("next_duration_sec_p50"),
        F.round(F.avg("cumulative_duration_sec"), 2).alias("avg_cumulative_duration_sec"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("min_next_duration_sec").desc_nulls_first())
)
