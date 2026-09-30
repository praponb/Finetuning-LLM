from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("level").orderBy("status_code_high", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host", "service")
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("host").orderBy(F.col("status_code").desc(), "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "region_or_default",
        F.last("region_or_default", ignorenulls=True).over(w_grp).alias("last_known_region_or_default"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.avg("status_code_high").over(w3).alias("host_avg_status_code_high"),
        F.ntile(4).over(w4).alias("status_code_bucket"),
    )
)
