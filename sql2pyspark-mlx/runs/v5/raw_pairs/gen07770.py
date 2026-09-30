from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "latency_ms",
        "status_code",
        "attr_key",
        F.count("status_code").over(w).alias("running_count_status_code"),
        F.sum("status_code").over(w).alias("running_sum_status_code"),
        F.max("status_code").over(w).alias("running_max_status_code"),
    )
)
