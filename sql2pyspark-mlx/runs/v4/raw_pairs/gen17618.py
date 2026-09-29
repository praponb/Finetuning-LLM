from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id")
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")
w5 = Window.orderBy("event_time", "id")
w6 = Window.partitionBy("session_browser").orderBy(F.col("seq_no").desc(), "id")
w7 = Window.partitionBy("session_country").orderBy("duration_sec", "id")
w_hist = Window.orderBy("next_duration_sec", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_hist_2 = Window.partitionBy("session_browser", "event_type").orderBy("seq_no", "id")

with_windows = (
    df_web_events
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("duration_sec_avg_30d", F.avg("duration_sec").over(w2))
    .withColumn("first_known_page", F.first("page", ignorenulls=True).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("next_duration_sec", F.lead("duration_sec", 1).over(w5))
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.browser"), F.col("p.country"), F.col("p.device"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.country").alias("session_country"),
        F.col("d.browser").alias("session_browser"),
        F.col("d.device").alias("session_device"),
    )
)

with_dims_web_events = (
    enriched
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.device").alias("session_device_2"),
        F.col("d.browser").alias("session_browser_2"),
        F.col("d.country").alias("session_country_2"),
    )
)

ranked = (
    with_dims_web_events
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    ranked
    .withColumn("next_next_duration_sec", F.lead("next_duration_sec", 1).over(w7))
    .withColumn("next_duration_sec_avg_5", F.avg("next_duration_sec").over(w_hist))
    .withColumn("event_seq_sum_7d", F.sum("event_seq").over(w9))
    .withColumn("next_next_duration_sec_2", F.lead("next_duration_sec", 1).over(w_hist_2))
)

joined = (
    metrics
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.device").alias("session_device_3"),
        F.col("d.country").alias("session_country_3"),
        F.col("d.browser").alias("session_browser_3"),
    )
)

result = (
    joined
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec_avg_30d",
        "seq_no",
        "next_duration_sec",
        "browser",
        "device",
        "session_country",
    )
    .orderBy(F.col("device").desc())
)
