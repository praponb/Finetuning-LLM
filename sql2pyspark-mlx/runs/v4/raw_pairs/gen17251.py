from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

result = (
    islands
    .withColumn("session_id_2_tier", F.when(F.col("session_id_2") >= 7, "PREMIUM").otherwise("STANDARD"))
    .rollup("session_id_2_tier")
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.max("event_time").alias("last_seen"),
        F.max("event_time").alias("last_event_time"),
        F.grouping("session_id_2_tier").alias("gid"),
    )
)
