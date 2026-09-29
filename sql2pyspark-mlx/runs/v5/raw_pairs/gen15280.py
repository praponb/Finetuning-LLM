from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("level", "service").orderBy("latency_ms")
w3 = Window.partitionBy("level", "service").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "host",
        "latency_ms",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.rank().over(w2).alias("latency_ms_rank"),
        F.percent_rank().over(w3).alias("status_code_percent_rank"),
        F.avg("latency_ms").over(w4).alias("latency_ms_avg_2h"),
        F.round((F.col("status_code") / F.sum("status_code").over(w5)), 4).alias("status_code_share"),
    )
)
