from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("num_context", "id")
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("status_code").desc(), "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        "context",
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        "num_context",
        "user_tier",
        F.row_number().over(w).alias("row_seq"),
        F.max("status_code").over(w2).alias("status_code_max_2h"),
        F.ntile(10).over(w3).alias("status_code_bucket"),
    )
)
