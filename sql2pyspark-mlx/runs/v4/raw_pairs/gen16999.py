from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    flattened
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w))
    .withColumn("k_max_latency_ms", F.max("latency_ms").over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
)
