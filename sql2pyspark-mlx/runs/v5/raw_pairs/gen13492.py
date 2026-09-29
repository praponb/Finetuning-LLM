from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("session_no") >= 9, "LARGE").otherwise("SMALL").alias("session_no_tier"),
        "customer_id",
        "session_no",
    )
    .agg(
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
        F.min("started_at").alias("started_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("session_no")).otherwise(0)).alias("chrome_session_no"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
    )
)
