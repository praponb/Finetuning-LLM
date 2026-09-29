from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("level").orderBy(F.col("has_fault").desc(), "id")
w3 = Window.partitionBy("host", "level").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("status_code", "id")
w5 = Window.partitionBy("level")

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        "context",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.sum("status_code").over(w).alias("host_sum_status_code"),
        F.ntile(10).over(w2).alias("has_fault_bucket"),
        F.sum("status_code").over(w3).alias("running_status_code"),
        F.ntile(5).over(w4).alias("status_code_bucket"),
        F.min("has_fault").over(w5).alias("level_min_has_fault"),
    )
)
