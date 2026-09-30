from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .withColumn("running_latency_ms", F.sum("latency_ms").over(w_ordered))
    .withColumn("prev_status_code", F.lag("status_code", 1).over(w_running))
)
