from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy("island_id", F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.avg("island_id").alias("avg_island_id"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("device") == "tablet", 1)), F.lit(0)), 2).alias("island_id_per_tablet"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("safari_count")
)
