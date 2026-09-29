from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host", "level").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .filter(F.col("latency_ms").isNotNull())
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    flattened_attrs
    .withColumn("first_known_latency_ms", F.first("latency_ms", ignorenulls=True).over(w))
    .withColumn("running_latency_ms", F.sum("latency_ms").over(w2))
)
