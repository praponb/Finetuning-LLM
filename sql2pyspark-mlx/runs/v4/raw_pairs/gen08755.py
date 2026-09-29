from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-05-05").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.concat("page", F.lit(" - "), "event_type").alias("page_event_type"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .withColumn("year_start", F.date_trunc("YEAR", "event_time"))
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time"))
    .cube("year_start", "activity_month")
    .agg(
        F.count("*").alias("event_count"),
        F.sum("new_group_flag").alias("total_new_group_flag"),
        F.min("event_time").alias("started_at"),
        F.count("*").alias("cnt"),
        F.grouping("year_start").alias("grouping_level"),
    )
    .orderBy(F.col("activity_month").asc_nulls_last())
)
