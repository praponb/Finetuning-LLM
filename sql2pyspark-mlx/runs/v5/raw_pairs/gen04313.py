from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier", "service").orderBy(F.col("log_ts").desc(), "id")
w2 = Window.partitionBy("tier", "service").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .filter(F.hour("log_ts").between(11, 16))
    .select(
        "id",
        "service",
        "log_ts",
        "payload_json",
        "context",
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .select(
        "id",
        "service",
        "log_ts",
        "duration_text",
        "tier",
        F.row_number().over(w).alias("seq_no"),
        F.first("tier", ignorenulls=True).over(w2).alias("first_known_tier"),
    )
)
