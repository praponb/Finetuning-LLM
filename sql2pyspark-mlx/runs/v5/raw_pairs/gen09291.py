from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "level").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.col("context").getItem("trace").alias("trace"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("tier") == "gold")
    .select(
        "id",
        "host",
        "level",
        F.min("status_code_high").over(w).alias("running_min_status_code_high"),
        F.count("status_code_high").over(w).alias("running_count_status_code_high"),
        F.max("status_code_high").over(w).alias("running_max_status_code_high"),
    )
)
