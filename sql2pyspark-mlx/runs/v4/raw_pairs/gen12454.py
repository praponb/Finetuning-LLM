from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-13 08:00:00").cast("timestamp")) & F.col("page").isNotNull() & (F.col("duration_sec") > 189))
    .select(
        "*",
        F.hour("event_time").alias("event_time_hour"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL").alias("new_group_flag_tier"),
    )
    .agg(
        F.min("event_time").alias("started_at"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.percentile_approx("event_time_hour", [0.25, 0.5, 0.75]).alias("event_time_hour_quartiles"),
        F.avg("event_time_hour").alias("avg_event_time_hour"),
        F.sum(F.when(F.col("event_type") == "click", F.col("event_time_hour")).otherwise(0)).alias("click_event_time_hour"),
        F.max(F.when(F.col("page") == "/checkout", F.col("event_time_hour"))).alias("checkout_max_event_time_hour"),
    )
)
