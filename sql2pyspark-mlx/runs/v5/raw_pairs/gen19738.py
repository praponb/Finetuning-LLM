from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")
w_hist = Window.orderBy("least_duration_sec_prev_duration_sec", "id")
w_hist_2 = Window.orderBy("least_duration_sec_prev_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
        F.substring("page", 1, 2).alias("page_prefix"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
        F.lag("duration_sec", 2).over(w).alias("prev_duration_sec_2"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("duration_sec") > 219)
    .withColumn("duration_sec_times_prev_duration_sec_2", (F.col("duration_sec") * F.col("prev_duration_sec_2")))
)

base = (
    cleaned
    .filter(F.col("event_time") > F.lit("2023-03-31 19:00:00").cast("timestamp"))
    .select(
        "id",
        "event_time",
        "duration_sec",
        "page_event_type_key",
        "page_prefix",
        "prev_duration_sec",
        "prev_duration_sec_2",
        F.date_trunc("MONTH", "event_time").alias("event_time_period"),
        F.least("duration_sec", "prev_duration_sec").alias("least_duration_sec_prev_duration_sec"),
    )
)

with_windows_web_events = (
    base
    .filter(F.col("least_duration_sec_prev_duration_sec").between(29, 77))
    .withColumn("next_prev_duration_sec", F.lead("prev_duration_sec").over(w_hist))
    .withColumn("prev_prev_duration_sec", F.lag("prev_duration_sec").over(w_hist))
    .withColumn("prev_least_duration_sec_prev_duration_sec", F.lag("least_duration_sec_prev_duration_sec").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("running_least_duration_sec_prev_duration_sec", F.sum("least_duration_sec_prev_duration_sec").over(w_hist_2))
)

result = (
    with_windows_web_events
    .select(
        "id",
        "prev_least_duration_sec_prev_duration_sec",
        "least_duration_sec_prev_duration_sec",
    )
    .unpivot(
        ids=["id"],
        values=["prev_least_duration_sec_prev_duration_sec", "least_duration_sec_prev_duration_sec"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
