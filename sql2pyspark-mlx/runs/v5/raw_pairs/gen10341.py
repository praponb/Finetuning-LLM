from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("level").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .filter(F.col("latency_ms") <= 2255)
    .select(
        "id",
        "level",
        "status_code",
        "key",
        "value",
        F.first("latency_ms", ignorenulls=True).over(w).alias("first_known_latency_ms"),
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
        F.sum("status_code").over(w3).alias("cumulative_status_code"),
    )
)
