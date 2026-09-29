from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("service", "level")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
)
