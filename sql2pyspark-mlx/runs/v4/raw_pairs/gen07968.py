from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id")
w2 = Window.orderBy("latency_ms", "id")
w3 = Window.partitionBy("service").orderBy("latency_ms", "id")

unpacked = (
    df_app_logs
    .filter(F.col("service") == "auth")
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "context",
        F.col("context").getItem("region").alias("region"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    unpacked
    .withColumn("next_latency_ms", F.lead("latency_ms", 2).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("latency_ms_bucket", F.ntile(5).over(w3))
    .withColumn("seq_no_2", F.row_number().over(w))
)
