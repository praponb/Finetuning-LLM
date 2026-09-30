from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("duration_sec", "id")
w2 = Window.partitionBy("level").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
    )
)

result = (
    flattened_attrs
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("first_known_version_or_default", F.first("version_or_default", ignorenulls=True).over(w2))
)
