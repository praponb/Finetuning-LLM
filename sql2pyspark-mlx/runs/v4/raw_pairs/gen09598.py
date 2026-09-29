from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("num_context").asc_nulls_first())
w3 = Window.orderBy("log_ts", "id")
w4 = Window.orderBy("latency_ms", "id")

unpacked = (
    df_app_logs
    .filter(F.col("log_ts") >= "2024-07-13")
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context").getItem("region").alias("region"),
        F.col("context").getItem("version").alias("version"),
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
    )
)

result = (
    unpacked
    .withColumn("cumulative_num_context", F.sum("num_context").over(w))
    .withColumn("num_context_rank", F.rank().over(w2))
    .withColumn("prev_status_code", F.lag("status_code", 1).over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("latency_ms_bucket", F.ntile(10).over(w4))
)
