from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

prev_events = base.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("country", "customer_id")
    .agg(
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no_2"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.min("started_at").alias("first_started_at"),
    )
)
