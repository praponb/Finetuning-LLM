from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_no") >= 8, "HIGH").otherwise("LOW").alias("session_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_rows"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
    )
    .filter(F.col("distinct_session_ids") >= 1)
    .orderBy(F.col("checkout_rows").asc_nulls_first())
)
