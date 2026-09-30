from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(~F.col("page").eqNullSafe(F.lit("/home")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.upper("event_type").alias("event_type_upper"),
    )
)

with_gaps = (
    base
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 500, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("event_type") == "purchase", 1)), F.lit(0)), 2).alias("new_group_flag_per_purchase"),
        F.countDistinct("page").alias("distinct_pages"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
    )
    .filter(F.col("distinct_pages") < 1000)
    .orderBy(F.col("duration_sec_bit_or").desc())
)
