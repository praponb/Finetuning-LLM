from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy("customer_id", "island_id")
    .agg(
        F.sum(F.when(F.col("country") == "DE", F.col("session_start_flag")).otherwise(0)).alias("de_session_start_flag"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("browser") == "Safari", 1)), F.lit(0)), 2).alias("island_id_per_safari"),
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
    )
    .orderBy(F.col("customer_id").asc_nulls_first())
)
