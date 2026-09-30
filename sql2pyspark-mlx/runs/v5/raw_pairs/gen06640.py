from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code_high", "id")
w2 = Window.partitionBy("host").orderBy(F.col("retry_count").asc_nulls_last())

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("status_code_high").between(1, 2))
    .select(
        "id",
        "host",
        "retry_count",
        "status_code_high",
        F.lead("status_code_high", 1).over(w).alias("next_status_code_high"),
        F.cume_dist().over(w2).alias("retry_count_cume_dist"),
    )
)
