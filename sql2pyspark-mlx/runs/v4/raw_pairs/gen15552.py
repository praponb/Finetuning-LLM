from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_share_3", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w2))
)
