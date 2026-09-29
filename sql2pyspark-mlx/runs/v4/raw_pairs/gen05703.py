from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_web_events
    .groupBy("page")
    .agg(
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.min("event_time").alias("first_event_time"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
    )
    .orderBy(F.col("page").asc_nulls_last())
)
