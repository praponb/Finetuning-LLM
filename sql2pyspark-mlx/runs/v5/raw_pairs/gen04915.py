from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("level", "host")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "log_ts",
        "latency_ms",
        "status_code",
        "attr_value",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_30m"),
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
        F.corr("latency_ms", "status_code").over(w3).alias("corr_latency_ms_status_code"),
    )
)
