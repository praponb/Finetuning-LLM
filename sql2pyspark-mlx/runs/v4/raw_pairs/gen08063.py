from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("duration_sec").between(386, 478))
    .select(
        "*",
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
        F.concat("event_type", F.lit("/"), "page").alias("event_type_page"),
    )
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("session_start_flag_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.max("event_time").alias("ended_at"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
    )
    .orderBy("session_start_flag_tier")
)
