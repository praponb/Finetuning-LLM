from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier")
w2 = Window.partitionBy("tier").orderBy("flag_bit4", "id")
w_recent = Window.partitionBy("service").orderBy("flag_bit4", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "context",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.sum("flag_bit4").over(w).alias("tier_sum_flag_bit4"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)
