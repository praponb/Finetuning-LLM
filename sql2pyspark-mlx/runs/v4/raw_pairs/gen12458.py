from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD"))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 458, "LARGE").otherwise("SMALL"))
    .rollup("is_new_session_tier", "duration_sec_tier")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("burst_id")).otherwise(0)).alias("purchase_burst_id"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.grouping("is_new_session_tier").alias("grouping_level"),
    )
    .orderBy(F.col("is_new_session_tier").desc())
)
