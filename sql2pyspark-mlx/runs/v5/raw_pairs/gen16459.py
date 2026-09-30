from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("host").orderBy("status_code")
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("host").orderBy(F.col("status_code").desc(), "id")

unpacked = (
    df_app_logs
    .filter(F.col("host") == "h1")
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "region",
        "user_tier",
        F.first("user_tier", ignorenulls=True).over(w).alias("first_known_user_tier"),
        F.percent_rank().over(w_ordered).alias("status_code_percent_rank"),
        F.corr("status_code", "latency_ms").over(w3).alias("corr_status_code_latency_ms"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
