from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id_tier").orderBy(F.col("avg_session_id").desc())

gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("session_id") >= 7, "HIGH").otherwise("LOW").alias("session_id_tier"),
        "session_id",
    )
    .agg(
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.min("started_at").alias("first_started_at"),
    )
    .withColumn("avg_session_id_dense_rank", F.dense_rank().over(w3))
)
