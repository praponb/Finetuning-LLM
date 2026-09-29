from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w2 = Window.partitionBy("attr_key", "level").orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.partitionBy("attr_key")
w4 = Window.partitionBy("level")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .filter(F.col("host") == "h1")
    .select(
        "latency_ms",
        "attr_key",
        "value",
        F.cume_dist().over(w).alias("latency_ms_cume_dist"),
        F.cume_dist().over(w2).alias("latency_ms_cume_dist_2"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share_2"),
    )
)
