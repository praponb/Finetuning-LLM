from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy("event_time_day")
w2 = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id")
w_hist = Window.orderBy(F.col("duration_sec").desc())
w4 = Window.partitionBy("event_type")
w_hist_2 = Window.partitionBy("page", "session_id").orderBy("event_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("page").orderBy(F.col("duration_sec").asc_nulls_first())
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

filtered = (
    df_web_events
    .filter((F.col("page").eqNullSafe(F.lit("/product"))) & (F.to_date("event_time") < "2023-10-01"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    latest
    .filter(F.col("session_id").isin([32, 38]))
    .select(
        "*",
        F.ntile(5).over(w2).alias("duration_sec_bucket"),
        F.dense_rank().over(w_hist).alias("duration_sec_dense_rank"),
        F.sum("duration_sec").over(w4).alias("event_type_sum_duration_sec"),
        F.last("duration_sec", ignorenulls=True).over(w_hist_2).alias("last_known_duration_sec"),
        F.percent_rank().over(w6).alias("duration_sec_percent_rank"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.duration_sec",
        "s.last_known_duration_sec",
        "s.duration_sec_percent_rank",
        F.col("b.browser"),
        F.col("b.country"),
        F.col("b.device"),
    )
)

result = (
    with_dims
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "last_known_duration_sec",
        "duration_sec_percent_rank",
        "browser",
        "country",
        "device",
        F.col("avg_web_event_duration_sec"),
    )
)
