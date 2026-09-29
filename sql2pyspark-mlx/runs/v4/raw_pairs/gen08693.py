from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "event_time").alias("activity_month"))
    .agg(
        F.max("event_time").alias("last_seen"),
        F.count_if(F.col("page") == "/checkout").alias("checkout_events"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
