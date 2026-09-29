from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("has_fault", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("level").orderBy("status_code", "id")
w4 = Window.partitionBy("level").orderBy("latency_ms", "id").rowsBetween(-5, Window.currentRow)

parsed = (
    df_app_logs
    .filter(F.hour("log_ts").between(7, 13))
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "context",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)

result = (
    parsed
    .select(
        "id",
        "status_code",
        "has_trace",
        F.avg("status_code").over(w).alias("status_code_avg_1h"),
        F.last("has_fault", ignorenulls=True).over(w2).alias("last_known_has_fault"),
        F.lead("has_fault", 2).over(w_running).alias("next_has_fault"),
        F.avg("status_code").over(w4).alias("status_code_avg_5"),
    )
)
