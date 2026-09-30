from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-10-20").cast("date")) & F.col("page").isNotNull())
    .select("*", F.round((F.col("duration_sec") * 100), 2).alias("duration_sec_scaled"))
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

result = (
    islands
    .cube(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
    .orderBy(F.col("duration_sec_bit_and").asc_nulls_first())
)
