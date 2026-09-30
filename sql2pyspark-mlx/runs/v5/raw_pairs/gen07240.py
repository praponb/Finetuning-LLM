from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier")
w2 = Window.orderBy(F.col("log_ts").desc(), "id")
w3 = Window.partitionBy("tier").orderBy("duration_sec", "id")

unpacked = (
    df_app_logs
    .filter(F.col("status_code") == 413)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "context",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.col("context")["version"].alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .filter(F.col("status_code_high") > 2)
    .select(
        "id",
        "log_ts",
        "is_flagged",
        "duration_sec",
        F.min("latency_ms").over(w).alias("tier_min_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.row_number().over(w2).alias("seq_no"),
        F.lead("latency_ms", 2, 0).over(w3).alias("next_latency_ms"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
