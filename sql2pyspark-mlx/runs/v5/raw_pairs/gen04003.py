from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("level").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("level").orderBy("num_context", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("level") != "WARN")
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "user_id",
        F.count("status_code").over(w_grp).alias("running_count_status_code"),
        F.avg("status_code").over(w_grp).alias("running_avg_status_code"),
        F.max("status_code").over(w_grp).alias("running_max_status_code"),
        F.last("latency_ms", ignorenulls=True).over(w_ordered).alias("last_known_latency_ms"),
    )
)
