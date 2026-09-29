from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc(), "id")
w3 = Window.partitionBy("service").orderBy(F.col("retry_count").desc(), "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code_high",
        "region_or_default",
        F.count("retry_count").over(w).alias("service_count_retry_count"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)
