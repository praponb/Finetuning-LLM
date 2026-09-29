from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("is_new_burst_tier").orderBy(F.col("edge_rows").desc())

gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
        F.date_trunc("WEEK", "started_at").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_rows"),
        F.percentile_approx("is_new_burst", 0.9).alias("is_new_burst_p90"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
    )
    .filter(F.col("is_new_burst_p90") > 0)
    .withColumn("edge_rows_dense_rank", F.dense_rank().over(w3))
)
