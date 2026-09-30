from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "context",
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("flag_bit4") >= 1, "LARGE").otherwise("SMALL").alias("flag_bit4_tier"),
    )
    .agg(
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.round(F.sum("flag_bit4") / F.nullif(F.count(F.when(F.col("host") == "h1", 1)), F.lit(0)), 2).alias("flag_bit4_per_h1"),
    )
    .orderBy(F.col("flag_bit4_tier").desc())
)
