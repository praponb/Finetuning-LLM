from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("tier").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("tier").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "context",
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .select(
        "id",
        "status_code",
        "region_or_default",
        "tier",
        F.max("status_code").over(w_running).alias("running_max_status_code"),
        F.avg("status_code").over(w_running).alias("running_avg_status_code"),
        F.min("status_code").over(w_running).alias("running_min_status_code"),
        F.sum("status_code").over(w_running).alias("running_sum_status_code"),
        F.avg("status_code").over(w2).alias("status_code_avg_1h"),
    )
)
