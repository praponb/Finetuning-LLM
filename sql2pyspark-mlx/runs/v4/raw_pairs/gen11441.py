from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("k", "host")
w3 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("status_code_share_2").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

kept_keys = (
    flattened
    .select("id", "host")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms") != 1744)
        .select("id", "host")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("k_max_status_code", F.max("status_code").over(w2))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w))
    .withColumn("status_code_share_2", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
)

scored_app_logs = (
    scored
    .withColumn("k_max_status_code_avg_2h", F.avg("k_max_status_code").over(w3))
    .withColumn("status_code_share_2_dense_rank", F.dense_rank().over(w4))
)

base = (
    scored_app_logs
    .filter(F.col("status_code_share_2_dense_rank").between(5, 6))
    .select(
        "*",
        F.substring("host", 1, 2).alias("host_prefix"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k"),
    )
)

result = (
    base
    .rollup(
        F.when(F.col("k_max_status_code") >= 467, "HIGH").otherwise("LOW").alias("k_max_status_code_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("host") == "h2", F.col("k_max_status_code")).otherwise(0)).alias("h2_k_max_status_code"),
    )
    .filter(F.col("span_minutes") >= 0)
    .orderBy(F.col("h2_k_max_status_code").desc())
)
