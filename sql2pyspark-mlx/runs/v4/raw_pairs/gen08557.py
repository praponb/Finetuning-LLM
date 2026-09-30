from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("service")
w_running = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_hist = Window.partitionBy("level")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.size("context").alias("num_context"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.stddev_samp("latency_ms").over(w_ordered).alias("service_stddev_samp_latency_ms"),
        F.max("num_context").over(w_running).alias("num_context_max_30m"),
        F.avg("num_context").over(w_hist).alias("level_avg_num_context"),
    )
)
