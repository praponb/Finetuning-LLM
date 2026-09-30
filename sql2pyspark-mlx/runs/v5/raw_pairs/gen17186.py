from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("key").orderBy(F.col("status_code_share").desc())
w_recent = Window.partitionBy("key").orderBy(F.col("status_code_share_dense_rank").desc_nulls_last())
w4 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

scored = (
    exploded
    .filter(F.col("key").isin(["trace", "region", "version"]))
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
    )
)

with_windows = (
    scored
    .withColumn("key_sum_status_code_share", F.sum("status_code_share").over(w))
    .withColumn("status_code_share_dense_rank", F.dense_rank().over(w2))
)

enriched = (
    with_windows
    .select(
        "id",
        "status_code",
        "key",
        "attr_value",
        "status_code_share",
        F.dense_rank().over(w_recent).alias("status_code_share_dense_rank_dense_rank"),
        F.max("status_code_share_dense_rank").over(w4).alias("status_code_share_dense_rank_max_15m"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("status_code_share_dense_rank_max_15m") >= 7, "LARGE").otherwise("SMALL").alias("status_code_share_dense_rank_max_15m_tier"),
        F.when(F.col("status_code_share") >= 0.62, "LARGE").otherwise("SMALL").alias("status_code_share_tier"),
    )
    .agg(
        F.avg(F.when(F.col("key") == "trace", F.col("status_code"))).alias("trace_avg_status_code"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.round(F.avg("status_code_share"), 2).alias("avg_status_code_share"),
    )
    .filter(F.col("status_code_stddev") > 0)
)
