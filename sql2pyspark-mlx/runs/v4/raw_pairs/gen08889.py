from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_web_events
    .groupBy("event_type")
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/search", 1)), F.lit(0)), 2).alias("duration_sec_per_search"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
    )
    .filter(F.col("avg_session_no") >= 0)
)
