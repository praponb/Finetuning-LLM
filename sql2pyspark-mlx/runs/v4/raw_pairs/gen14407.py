from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
    )
    .agg(
        F.min("session_id").alias("min_session_id"),
        F.max(F.when(F.col("device") == "tablet", F.col("session_start_flag"))).alias("tablet_max_session_start_flag"),
    )
    .filter(F.col("tablet_max_session_start_flag") < 0)
    .orderBy("min_session_id")
)
