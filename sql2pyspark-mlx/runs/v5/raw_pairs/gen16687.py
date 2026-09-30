from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("session_id_2"),
    )
)

result = (
    sessionized
    .groupBy("event_type")
    .agg(
        F.percentile_approx("session_id_2", [0.25, 0.5, 0.75]).alias("session_id_2_quartiles"),
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec"))).alias("cart_total_duration_sec"),
        F.max("event_time").alias("ended_at"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .filter(F.col("span_minutes") > 0)
    .orderBy("event_type")
)
