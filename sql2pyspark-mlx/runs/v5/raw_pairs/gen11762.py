from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code", "id")

parsed = (
    df_app_logs
    .filter(F.col("status_code") >= 376)
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("unknown")).alias("trace_or_default"),
    )
)

result = (
    parsed
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w))
    .withColumn("prev_latency_ms", F.lag("latency_ms").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)
