from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("session_no", "id")
w4 = Window.orderBy(F.col("session_no").desc(), "id")

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "session_id",
        "event_time",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

latest = (
    sessionized
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "event_time", "session_no")
)

with_windows = (
    latest
    .select(
        "*",
        F.row_number().over(w_grp).alias("event_seq"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("event_time") < "2025-09-01")
    .select("*", F.coalesce("event_seq", F.lit(0)).alias("event_seq_filled"))
)

result = cleaned.select("id", "session_no", "event_seq", "event_seq_filled")
