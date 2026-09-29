from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_rank", F.rank().over(w2))
)
