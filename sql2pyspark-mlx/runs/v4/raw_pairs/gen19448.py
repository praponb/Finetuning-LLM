from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host").orderBy("has_fault", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("has_fault", "id")

flattened_attrs = (
    df_app_logs
    .filter(F.col("log_ts") >= "2023-11-21")
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.col("context")["version"].alias("version"),
        F.col("context")["region"].alias("region"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.last("latency_ms", ignorenulls=True).over(w).alias("last_known_latency_ms"),
        F.sum("has_fault").over(w2).alias("running_has_fault"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
