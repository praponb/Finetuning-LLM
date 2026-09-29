from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("country", "browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("country") == "SG")
    .withColumn("device_length", F.length("device"))
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

with_windows = (
    sessions_sessions
    .withColumn("session_start_flag_sum_90d", F.sum("session_start_flag").over(w3))
    .withColumn("device_length_avg_7d", F.avg("device_length").over(w4))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("session_no") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
    )
    .agg(
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
    .filter(F.col("num_countries") >= 1)
    .orderBy("th_count")
)
