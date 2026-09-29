from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "attr_key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .select(
        "attr_key",
        "is_flagged",
        F.count("status_code_high").over(w).alias("attr_key_count_status_code_high"),
    )
)

valid = (
    enriched
    .filter(F.col("attr_key").isin(["version", "region"]))
    .withColumn("attr_key_lower", F.lower("attr_key"))
    .withColumn("adjusted_attr_key_count_status_code_high", F.round((F.col("attr_key_count_status_code_high") * 1.15), 2))
)

result = (
    valid
    .select(
        "attr_key",
        "attr_key_lower",
        F.when(F.col("attr_key").rlike("^(version|region)$"), "CORE").when(F.col("attr_key").rlike("^(trace)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("attr_key_zone"),
    )
)
