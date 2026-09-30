from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("attr_key_length").desc_nulls_first(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

unpacked = (
    lines
    .filter(~F.col("attr_key").isin(["region", "version"]))
    .select(
        "id",
        "level",
        "status_code",
        "attr_key",
        "attr_value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    unpacked
    .filter(F.col("attr_key").isNotNull())
    .select("*", F.length("attr_key").alias("attr_key_length"))
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "level",
        "status_code",
        "attr_key",
        "attr_value",
        "status_code_high",
        "attr_key_length",
        F.when(F.col("attr_key").rlike("^(version|trace)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("attr_key_zone"),
    )
)
