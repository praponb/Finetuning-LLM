from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .filter(F.col("log_ts") >= "2023-11-01")
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "key",
        "value",
        F.max("latency_ms").over(w_hist).alias("latency_ms_max_2h"),
        F.rank().over(w2).alias("latency_ms_rank"),
    )
)
