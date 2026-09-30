from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc())
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w))
    .withColumn("latency_ms_cume_dist_2", F.cume_dist().over(w2))
)
