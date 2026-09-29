from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy(F.date_trunc("quarter", "event_time").alias("quarter_start"))
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.count_if(F.col("event_type") == "add_to_cart").alias("add_to_cart_events"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.size(F.collect_set("event_type")).alias("num_event_types_2"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or_2"),
    )
)
