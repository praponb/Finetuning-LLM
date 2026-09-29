from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy("country", "customer_id").orderBy(F.col("is_new_burst").desc())

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-10-09").cast("date")) & (F.col("browser") == "Edge"))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.length("device").alias("device_length"),
    )
)

with_gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

metrics = (
    sessions_sessions
    .withColumn("prev_is_new_burst", F.lag("is_new_burst", 2).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("is_new_burst_percent_rank", F.percent_rank().over(w5))
)

eligible = (
    metrics
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page").isNotNull()), "left_semi")
)

result = (
    eligible
    .groupBy("burst_id")
    .agg(
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        F.max("started_at").alias("last_started_at"),
    )
    .filter(F.col("avg_is_new_burst") < 0)
    .orderBy(F.col("burst_id").desc_nulls_last())
)
