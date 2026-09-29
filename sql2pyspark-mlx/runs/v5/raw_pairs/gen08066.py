from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy("session_no")
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
    .orderBy("num_event_types")
)
