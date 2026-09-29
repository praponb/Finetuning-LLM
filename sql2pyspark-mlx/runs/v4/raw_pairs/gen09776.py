from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("session_id").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("is_new_session").desc_nulls_first())
w5 = Window.partitionBy("browser")
w6 = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy(F.col("session_id_sum_7d").desc(), "id")

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-07-08"))) & F.col("country").isNotNull())
    .select(
        "*",
        F.trim("country").alias("clean_country"),
        F.col("started_at").cast("date").alias("started_at_day"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_clean"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

latest = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    latest
    .withColumn("is_new_session_percent_rank", F.percent_rank().over(w4))
    .withColumn("browser_sum_is_new_session", F.sum("is_new_session").over(w5))
    .withColumn("session_id_sum_7d", F.sum("session_id").over(w6))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at_day")), "left_anti")
)

enriched = (
    not_flagged
    .withColumn("is_new_session_percent_rank_max_7d", F.max("is_new_session_percent_rank").over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("browser_stddev_samp_session_id_sum_7d", F.stddev_samp("session_id_sum_7d").over(w5))
)

kept = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.session_id").isin([8, 34])), "left_anti")
)

wide = (
    kept
    .groupBy("country", "device")
    .pivot("browser", ["Chrome", "Safari"])
    .agg(F.max("browser_stddev_samp_session_id_sum_7d").alias("total"))
)

result = (
    wide
    .select(
        "country",
        "device",
        "Chrome",
        "Safari",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("device_zone"),
    )
    .orderBy("Safari")
)
