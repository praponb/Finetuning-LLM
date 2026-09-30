from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .withColumn("latency_ms_rank", F.rank().over(w))
    .withColumn("latency_ms_avg_1h", F.avg("latency_ms").over(w2))
    .withColumn("latency_ms_rank_2", F.rank().over(w3))
)
