from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "k").orderBy("status_code", "id", "k")
w2 = Window.partitionBy("k").orderBy("status_code_str", "id", "k")
w3 = Window.orderBy(F.col("status_code_str").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

base = (
    exploded
    .filter(~F.col("k").isin(["trace", "version"]) & (F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 16:00:00"))))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        F.col("status_code").cast("double").alias("status_code_str"),
    )
)

cleaned = (
    base
    .filter(F.col("k") == "trace")
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "k",
        "attr_value",
        "status_code_str",
        F.concat("k", F.lit(" "), "level").alias("k_level"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.ntile(4).over(w2).alias("status_code_str_bucket"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

base_app_logs = (
    enriched
    .filter(F.col("status_code_str") > 45)
    .select(
        "id",
        "level",
        "status_code",
        "k",
        "k_level",
        "resolved_k",
        "rank_in_group",
        "status_code_str_bucket",
        "row_seq",
        F.col("status_code_str_bucket").cast("double").alias("status_code_str_bucket_str"),
        F.coalesce("k", F.lit("OTHER")).alias("k_clean"),
    )
)

result = (
    base_app_logs
    .groupBy("level")
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("k") == "version", 1)), F.lit(0)), 2).alias("row_seq_per_version"),
    )
    .orderBy("row_seq_per_version")
)
