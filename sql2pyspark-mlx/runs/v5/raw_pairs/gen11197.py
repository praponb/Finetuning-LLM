from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "status_code",
        "attr_key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

enriched = (
    unpacked
    .filter(F.col("attr_key").isin(["version", "region", "trace"]))
    .select(
        "id",
        "status_code",
        "attr_key",
        "v",
        "has_fault",
        F.dense_rank().over(w_running).alias("status_code_dense_rank"),
        F.rank().over(w2).alias("status_code_high_rank"),
    )
)

matched_keys = (
    enriched
    .select("id")
    .exceptAll(
        df_app_logs
        .filter(F.col("host").isin(["h3", "h1", "h2"]))
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "status_code",
        "attr_key",
        "v",
        "has_fault",
        F.when(F.col("attr_key").rlike("^(version|region)$"), "OTHER_ZONE").otherwise("CORE").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key_zone").desc_nulls_last(), "has_fault")
)
