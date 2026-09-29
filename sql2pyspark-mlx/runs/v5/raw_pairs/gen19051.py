from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("flag_bit8", "id").rowsBetween(-3, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .filter(F.hour("log_ts").between(8, 14))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("flag_bit8").isNotNull())
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "has_version",
        "region",
        F.min("flag_bit8").over(w).alias("last4_min_flag_bit8"),
        F.avg("flag_bit8").over(w).alias("last4_avg_flag_bit8"),
        F.count("flag_bit8").over(w).alias("last4_count_flag_bit8"),
    )
)
