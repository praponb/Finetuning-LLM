from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy("status_code")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

flattened_attrs = (
    flattened
    .filter(F.col("status_code") >= 369)
    .select(
        "id",
        "host",
        "status_code",
        "attr_key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

metrics = (
    flattened_attrs
    .filter(F.col("is_flagged").between(0, 1))
    .select(
        "id",
        "status_code",
        "v",
        F.avg("is_flagged").over(w).alias("host_avg_is_flagged"),
        F.dense_rank().over(w2).alias("status_code_dense_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("host_avg_is_flagged") < 0)
    .select(
        "*",
        F.ceil("status_code_dense_rank").alias("status_code_dense_rank_whole"),
        F.col("status_code_dense_rank").cast("double").alias("status_code_dense_rank_num"),
    )
)

valid = (
    cleaned
    .filter(F.col("status_code_dense_rank_num").isNull())
    .select(
        "v",
        "host_avg_is_flagged",
        "status_code_dense_rank_whole",
        F.ceil("status_code_dense_rank_whole").alias("status_code_dense_rank_whole_whole"),
    )
)

filtered = (
    valid
    .filter(F.col("host_avg_is_flagged") < 0)
    .select(
        "*",
        F.when(F.col("host_avg_is_flagged") >= 1, "High").when(F.col("host_avg_is_flagged") >= 0, "Medium").otherwise("Low").alias("host_avg_is_flagged_band"),
    )
)

base = (
    filtered
    .filter(F.col("status_code_dense_rank_whole_whole") < 37)
    .select(
        "*",
        F.coalesce("status_code_dense_rank_whole", F.lit(0)).alias("status_code_dense_rank_whole_filled"),
        F.round((F.col("status_code_dense_rank_whole") * 0.85), 2).alias("status_code_dense_rank_whole_scaled"),
    )
)

result = (
    base
    .select(
        "status_code_dense_rank_whole",
        "status_code_dense_rank_whole_whole",
        "host_avg_is_flagged_band",
        "status_code_dense_rank_whole_filled",
    )
    .orderBy(F.col("status_code_dense_rank_whole").desc_nulls_last())
)
