from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        "browser",
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
        "country",
    )
    .agg(
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.max("started_at").alias("ended_at"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
        F.min("started_at").alias("first_started_at"),
    )
    .filter(F.col("island_id_p95") > 0)
)
