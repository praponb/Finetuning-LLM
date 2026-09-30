from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("latency_ms").desc())
w3 = Window.orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .filter(F.col("latency_ms") < 643)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "trace_or_default",
        F.count("latency_ms").over(w).alias("latency_ms_count_2h"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
        F.first("trace_or_default", ignorenulls=True).over(w3).alias("first_known_trace_or_default"),
    )
)
