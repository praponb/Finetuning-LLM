from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.orderBy("latency_ms", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.rank().over(w2).alias("latency_ms_rank"),
        F.ntile(3).over(w3).alias("latency_ms_bucket"),
    )
)
