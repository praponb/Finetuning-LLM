from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        "page",
        F.when(F.col("burst_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("burst_id"))).alias("purchase_total_burst_id"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles_2"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .filter(F.col("purchase_total_burst_id") > 0)
)
