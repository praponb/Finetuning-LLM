from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("attr_key", "host").orderBy(F.col("status_code_scaled").desc(), "id", "attr_key")
w3 = Window.partitionBy("host").orderBy(F.col("status_code_dense_rank").desc_nulls_first(), "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "host",
        "status_code",
        "attr_key",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    unpacked
    .filter(F.col("attr_key").isNotNull() & F.col("host").isin(["h3", "h4"]))
    .withColumn("attr_key_host", F.concat("attr_key", F.lit(", "), "host"))
    .withColumn("status_code_scaled", F.round((F.col("status_code") * 1.15), 2))
)

with_windows = (
    base
    .select(
        "id",
        "host",
        "status_code",
        "attr_key",
        F.dense_rank().over(w_part).alias("status_code_dense_rank"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

grouped = (
    latest
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["host", "attr_key", "event_seq_tier"], ["host", "attr_key"], []],
        "host",
        "attr_key",
        "event_seq_tier",
    )
    .agg(
        F.percentile_approx("status_code_dense_rank", 0.9).alias("status_code_dense_rank_p90"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.sum(F.when(F.col("attr_key") == "region", F.col("status_code_dense_rank")).otherwise(0)).alias("region_status_code_dense_rank"),
        F.round(F.avg("status_code_dense_rank"), 2).alias("avg_status_code_dense_rank"),
        F.size(F.collect_set("host")).alias("num_hosts"),
    )
)

result = (
    grouped
    .select("attr_key", "status_code_dense_rank_p90")
    .orderBy(F.col("status_code_dense_rank_p90").desc())
)
