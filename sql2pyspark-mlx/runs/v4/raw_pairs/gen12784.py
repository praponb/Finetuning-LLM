from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id", "key").rowsBetween(-3, -1)
w2 = Window.partitionBy("level").orderBy("latency_ms", "id", "key").rowsBetween(-2, -1)
w3 = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .withColumn("latency_ms_avg_3_prior", F.avg("latency_ms").over(w))
    .withColumn("latency_ms_avg_2_prior", F.avg("latency_ms").over(w2))
    .withColumn("latency_ms_rank", F.rank().over(w3))
)
