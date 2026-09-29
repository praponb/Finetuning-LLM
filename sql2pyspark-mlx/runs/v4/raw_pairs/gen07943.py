from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code", "id")
w2 = Window.partitionBy("service", "host").orderBy("status_code", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    flattened_attrs
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)
