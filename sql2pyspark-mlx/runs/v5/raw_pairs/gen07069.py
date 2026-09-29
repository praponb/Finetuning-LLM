from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "LARGE").otherwise("SMALL").alias("session_start_flag_tier"),
    )
    .agg(
        F.sum(F.when(F.col("device") == "desktop", F.col("session_id"))).alias("desktop_total_session_id"),
        F.percentile_approx("session_id", 0.9).alias("session_id_p90"),
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("session_id_p90") >= 0)
    .orderBy(F.col("event_count").asc_nulls_last())
)
