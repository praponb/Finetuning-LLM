from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("quarter_start").orderBy(F.col("chrome_new_group_flag").desc())

gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.date_trunc("quarter", "started_at").alias("quarter_start"),
        F.date_trunc("month", "started_at").alias("activity_month"),
        F.when(F.col("session_no") >= 7, "LARGE").otherwise("SMALL").alias("session_no_tier"),
    )
    .agg(
        F.sum(F.when(F.col("browser") == "Chrome", F.col("new_group_flag")).otherwise(0)).alias("chrome_new_group_flag"),
        F.sum(F.when(F.col("device") == "desktop", F.col("new_group_flag")).otherwise(0)).alias("desktop_new_group_flag"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles_2"),
    )
    .withColumn("chrome_new_group_flag_dense_rank", F.dense_rank().over(w3))
    .orderBy(F.col("avg_session_no").asc_nulls_first())
)
