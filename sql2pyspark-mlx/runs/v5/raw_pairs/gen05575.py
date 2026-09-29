from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("has_fault", "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "payload_json",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    unpacked
    .filter(F.col("status_code") >= 308)
    .select(
        "id",
        "log_ts",
        "status_code_high",
        F.count("status_code").over(w_part).alias("status_code_count_2h"),
        F.ntile(5).over(w2).alias("has_fault_bucket"),
    )
)
