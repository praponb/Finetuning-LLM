from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("status_code_high").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    parsed
    .filter(~F.col("key").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "key",
        "flag_bit4",
        "status_code_high",
        F.coalesce("key", F.lit("Unknown")).alias("key_filled"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

base = (
    cleaned
    .filter(F.col("key") == "trace")
    .select(
        "*",
        F.col("flag_bit4").cast("string").alias("flag_bit4_str"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean_2"),
    )
)

enriched = (
    base
    .filter(F.col("flag_bit4") >= 1)
    .withColumn("status_code_high_percent_rank", F.percent_rank().over(w))
)

result = (
    enriched
    .select("flag_bit4_str", "key_clean_2", "status_code_high_percent_rank")
    .orderBy("status_code_high_percent_rank")
)
