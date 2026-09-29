from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("level").orderBy("log_ts", "id")
w_running = Window.partitionBy("host").orderBy(F.col("log_ts").desc(), "id")
w_ordered = Window.partitionBy("level", "host").orderBy("status_code", "id")

parsed = (
    df_app_logs
    .filter(F.col("service") == "auth")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    parsed
    .filter(F.col("retry_count") == 2)
    .select(
        "id",
        "log_ts",
        "level",
        "version",
        "user_tier",
        "trace",
        "retry_count",
        F.avg("retry_count").over(w).alias("retry_count_avg_6"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.row_number().over(w_running).alias("row_seq"),
        F.lead("status_code", 1).over(w_ordered).alias("next_status_code"),
    )
)
