from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull() & (F.col("started_at") < "2024-11-19"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.trim("device").alias("clean_device"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("burst_id") >= 7, "HIGH").otherwise("LOW").alias("burst_id_tier"),
        F.date_trunc("week", "started_at").alias("week_start"),
    )
    .agg(
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
    )
    .filter(F.col("avg_burst_id") > 0)
)
