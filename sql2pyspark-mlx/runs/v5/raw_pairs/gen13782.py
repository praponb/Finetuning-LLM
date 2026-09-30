from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    parsed
    .select(
        "id",
        "log_ts",
        "status_code_high",
        "duration_sec",
        "version",
        F.avg("status_code").over(w).alias("running_avg_status_code"),
        F.min("status_code").over(w).alias("running_min_status_code"),
        F.max("status_code").over(w).alias("running_max_status_code"),
    )
)
