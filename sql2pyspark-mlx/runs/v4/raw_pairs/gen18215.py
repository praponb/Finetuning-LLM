from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("duration_sec") <= 327)
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.lower("page").alias("page_lower"),
        F.coalesce("page", F.lit("N/A")).alias("resolved_page"),
    )
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .rollup(F.date_trunc("MONTH", "event_time").alias("activity_month"), "session_no")
    .agg(
        F.max("event_time").alias("last_seen"),
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("event_type") == "purchase", 1)), F.lit(0)), 2).alias("new_group_flag_per_purchase"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.max("event_time").alias("last_event_time"),
        F.min("event_time").alias("first_seen"),
    )
    .filter(F.col("new_group_flag_per_purchase") > 0)
)
