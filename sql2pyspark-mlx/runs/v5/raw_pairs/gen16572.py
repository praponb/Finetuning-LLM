from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .filter(F.col("attr_key").isin(["trace", "version"]))
    .select(
        "*",
        F.sum("status_code").over(w_running).alias("status_code_sum_1h"),
        F.count("status_code").over(w_running).alias("status_code_count_1h"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.max("status_code").over(w3).alias("status_code_max_15m"),
    )
)
