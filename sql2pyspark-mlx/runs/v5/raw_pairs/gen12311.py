from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("page").isin(["/product", "/search"]))
    .withColumn("page_title", F.initcap(F.trim("page")))
    .withColumn("resolved_page", F.coalesce("page", F.lit("DIRECT")))
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
        F.when(F.col("session_id_2") >= 9, "PREMIUM").otherwise("STANDARD").alias("session_id_2_tier"),
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max("event_time").alias("last_seen"),
    )
    .filter(F.col("avg_duration_sec") > 0)
    .orderBy(F.col("avg_duration_sec").desc_nulls_last())
)
