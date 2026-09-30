from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code", "id", "key")
w2 = Window.orderBy("status_code", "id", "key")
w3 = Window.partitionBy("service").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.partitionBy("service").orderBy("log_ts", "id", "key").rowsBetween(-5, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "status_code",
        "key",
        "attr_value",
        F.lag("status_code", 2).over(w).alias("prev_status_code"),
        F.lag("status_code", 1).over(w2).alias("prev_status_code_2"),
        F.sum("status_code").over(w3).alias("running_status_code"),
        F.max("status_code").over(w4).alias("status_code_max_2h"),
        F.avg("status_code").over(w_running).alias("status_code_avg_5"),
    )
)
