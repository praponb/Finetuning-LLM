from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("latency_ms", "id")
w_running = Window.partitionBy("level", "service").orderBy("is_flagged", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context")["trace"].alias("trace"),
        F.col("context")["version"].alias("version"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    unpacked
    .withColumn("prev_latency_ms", F.lag("latency_ms", 1).over(w))
    .withColumn("running_is_flagged", F.sum("is_flagged").over(w_running))
)
