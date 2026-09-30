from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("h4_max_status_code_high").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

unpacked = (
    lines
    .filter(F.col("host").isin(["h4", "h1"]))
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "attr_key",
        "value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    unpacked
    .filter(F.col("flag_bit2") > 1)
    .select(
        "id",
        "host",
        "attr_key",
        "value",
        "status_code_high",
        F.concat_ws("/", "host", "service").alias("host_service_key"),
    )
)

result = (
    valid
    .groupBy(
        "attr_key",
        F.when(F.col("status_code_high") >= 2, "HIGH").otherwise("LOW").alias("status_code_high_tier"),
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_count"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.round(F.sum("status_code_high") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("status_code_high_per_region"),
        F.max(F.when(F.col("host") == "h4", F.col("status_code_high"))).alias("h4_max_status_code_high"),
    )
    .withColumn("h4_max_status_code_high_rank", F.rank().over(w))
)
