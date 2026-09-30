from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("status_code", "id", "k").rowsBetween(-5, -1)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    flattened
    .filter(F.col("k") == "trace")
    .select(
        "id",
        "service",
        "latency_ms",
        "k",
        "v",
        F.last("k", ignorenulls=True).over(w).alias("last_known_k"),
        F.avg("status_code").over(w2).alias("status_code_avg_5_prior"),
    )
)
