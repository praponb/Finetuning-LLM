from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("level").orderBy("latency_ms", "id")
w2 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context").getItem("version").alias("version"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "service",
        "version",
        F.lag("duration_sec", 2).over(w_hist).alias("prev_duration_sec"),
        F.sum("num_context").over(w2).alias("cumulative_num_context"),
        F.min("latency_ms").over(w3).alias("service_min_latency_ms"),
    )
)
