from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("event_type_page", F.concat("event_type", F.lit(", "), "page"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("OTHER")))
)

with_prev = valid.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("duration_sec") >= 524, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        F.date_trunc("YEAR", "prev_event_time").alias("year_start"),
    )
    .agg(
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        ((F.unix_timestamp(F.max("prev_event_time")) - F.unix_timestamp(F.min("prev_event_time"))) / 60).alias("span_minutes"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
    .filter(F.col("duration_sec_bit_or") > 1)
)
