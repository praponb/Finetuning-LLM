from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.to_date("log_ts") < "2023-06-01")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "payload_json",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
    )
)
