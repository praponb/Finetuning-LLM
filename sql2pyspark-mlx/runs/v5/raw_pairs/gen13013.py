from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")
w_part = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_hist = Window.partitionBy("event_type").orderBy("event_time", "id")
w5 = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w6 = Window.partitionBy("event_type").orderBy(F.col("duration_sec_bucket").desc_nulls_first(), "id")
w7 = Window.partitionBy("browser").orderBy("event_time", "id")
w8 = Window.partitionBy("browser").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_web_events
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.sum("duration_sec").over(w_part).alias("running_duration_sec"),
        F.max("duration_sec").over(w_grp).alias("duration_sec_max_90d"),
        F.lead("duration_sec", 2).over(w_hist).alias("next_duration_sec"),
        F.ntile(10).over(w5).alias("duration_sec_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    latest
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.browser"))
)

flagged = (
    with_dims
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w7) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "event_type",
        "event_time",
        "row_seq",
        "duration_sec_bucket",
        "browser",
        F.sum("session_start_flag").over(w8).alias("session_no"),
    )
)

result = (
    sessionized
    .select(
        "id",
        "event_type",
        "event_time",
        "row_seq",
        "browser",
        "session_no",
        F.when(F.col("browser").rlike("^(Firefox|Safari)$"), "GROUP_B").when(F.col("browser").rlike("^(Chrome|Edge)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("browser_zone"),
    )
)
