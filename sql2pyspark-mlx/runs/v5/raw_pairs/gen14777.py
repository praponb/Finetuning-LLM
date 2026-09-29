from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("status_code_high").desc_nulls_last(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

valid = (
    parsed
    .filter(F.col("host") == "h1")
    .select(
        "*",
        F.length("host").alias("host_length"),
        F.when(F.col("status_code_high") >= 2, "High").when(F.col("status_code_high") >= 1, "Medium").otherwise("Low").alias("status_code_high_tier"),
        F.coalesce("key", F.lit("OTHER")).alias("key_filled"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

matched_keys = (
    deduped
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.to_date("log_ts") < "2025-04-01")
        .select("id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "key",
        "v",
        "host_length",
        "status_code_high_tier",
        "key_filled",
        F.when(F.col("service").rlike("^(search|checkout)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("service_zone"),
    )
    .orderBy(F.col("service_zone").desc_nulls_first())
)
