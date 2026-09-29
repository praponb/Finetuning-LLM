from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    gaps
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessions_sessions
    .groupBy("country")
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.min("started_at").alias("started_at"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("island_id_per_th"),
    )
    .orderBy(F.col("island_id_per_th").desc())
)
