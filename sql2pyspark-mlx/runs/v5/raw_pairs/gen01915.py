from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec_bit_and").desc())

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy("page", "session_id")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
    .withColumn("duration_sec_bit_and_dense_rank", F.dense_rank().over(w3))
)
