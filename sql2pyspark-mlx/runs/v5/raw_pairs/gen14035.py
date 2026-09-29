from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code", "id")
w2 = Window.partitionBy("service").orderBy("num_context", "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.size("context").alias("num_context"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    parsed
    .filter(F.col("service") == "billing")
    .withColumn("status_code_bucket", F.ntile(10).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)
