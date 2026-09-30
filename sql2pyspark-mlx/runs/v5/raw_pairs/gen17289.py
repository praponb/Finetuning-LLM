from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("event_type", "session_id").orderBy("event_time_day", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").desc(), "id")
w_running = Window.orderBy("event_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("session_id") == 28)
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.col("event_time").cast("date").alias("event_time_day"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

result = (
    valid
    .select(
        "id",
        "session_id",
        "event_time_day",
        F.sum("duration_sec").over(w_recent).alias("duration_sec_sum_30d"),
        F.lag("duration_sec", 1).over(w2).alias("prev_duration_sec"),
        F.first("duration_sec", ignorenulls=True).over(w3).alias("first_known_duration_sec"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.sum("duration_sec").over(w_running).alias("cumulative_duration_sec"),
    )
)
