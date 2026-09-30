from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_6"),
        F.ntile(3).over(w2).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_3"),
    )
)

result = (
    scored
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        F.percentile_approx("duration_sec_avg_6", 0.9).alias("duration_sec_avg_6_p90"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("duration_sec_avg_6_p90").desc_nulls_last())
)
