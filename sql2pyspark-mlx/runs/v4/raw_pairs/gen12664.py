from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id")
w_ordered_2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

valid = (
    df_web_events
    .filter(F.col("duration_sec").between(162, 440))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.coalesce("page", F.lit("Unknown")).alias("page_filled"),
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
    )
)

metrics = (
    valid
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w_ordered_2))
    .withColumn("cumulative_duration_sec_2", F.sum("duration_sec").over(w3))
    .withColumn("duration_sec_avg_3_prior", F.avg("duration_sec").over(w_hist))
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("year", "event_time").alias("year_start"))
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "click", 1)), F.lit(0)), 2).alias("duration_sec_per_click"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
    )
    .orderBy("year_start")
)
