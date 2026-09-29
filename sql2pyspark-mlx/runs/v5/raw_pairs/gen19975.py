from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code", "id", "k")
w2 = Window.orderBy("latency_ms", "id", "k").rowsBetween(-3, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "latency_ms",
        "k",
        F.lag("status_code", 2).over(w).alias("prev_status_code"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_3"),
    )
)
