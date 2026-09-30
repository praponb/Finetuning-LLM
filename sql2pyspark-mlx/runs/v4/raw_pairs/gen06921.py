from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("service", "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.orderBy(F.col("latency_ms").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("key_count_latency_ms", F.count("latency_ms").over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
    .withColumn("latency_ms_sum_2h", F.sum("latency_ms").over(w4))
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w5))
)
