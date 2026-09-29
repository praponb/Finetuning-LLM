from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("latency_ms")
w2 = Window.partitionBy("service").orderBy("latency_ms")
w3 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc())
w4 = Window.orderBy(F.col("latency_ms").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("latency_ms_rank", F.rank().over(w))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w2))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w3))
    .withColumn("latency_ms_rank_2", F.rank().over(w4))
)
