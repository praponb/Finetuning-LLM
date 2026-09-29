from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_sessions
    .groupBy(
        "device",
        F.when(F.col("session_id") >= 9, "PREMIUM").otherwise("STANDARD").alias("session_id_tier"),
    )
    .agg(
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
    )
)
