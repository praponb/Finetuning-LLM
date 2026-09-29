from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-03-12").cast("date")) & (F.col("page").eqNullSafe(F.lit("/search"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.trim("event_type").alias("clean_event_type"),
    )
)

prev_events = base.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .groupingSets(
        [["session_no", "event_type"], ["session_no"], []],
        "session_no",
        "event_type",
    )
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "search", 1)), F.lit(0)), 2).alias("duration_sec_per_search"),
        F.grouping("session_no").alias("grouping_level"),
    )
    .orderBy(F.col("checkout_count").desc_nulls_first())
)
