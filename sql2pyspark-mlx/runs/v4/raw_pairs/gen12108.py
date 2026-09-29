from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms")
w2 = Window.orderBy(F.col("latency_ms").asc_nulls_last())
w3 = Window.partitionBy("level", "k")
w4 = Window.partitionBy("k", "level").orderBy(F.col("latency_ms").desc_nulls_first())
w5 = Window.partitionBy("k", "level")

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w))
    .withColumn("latency_ms_percent_rank_2", F.percent_rank().over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
    .withColumn("latency_ms_percent_rank_3", F.percent_rank().over(w4))
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w5)), 4))
)
