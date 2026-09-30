from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("log_ts_weekofyear").desc_nulls_first())
w4 = Window.partitionBy("k")
w5 = Window.partitionBy("k").orderBy(F.col("status_code_max_2h").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

metrics = (
    lines
    .select(
        "id",
        "log_ts",
        "status_code",
        "k",
        F.max("status_code").over(w_ordered).alias("status_code_max_2h"),
        F.avg("status_code").over(w2).alias("status_code_avg_1h"),
    )
)

cleaned = (
    metrics
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .withColumn("status_code_plus_status_code_max_2h", (F.col("status_code") + F.col("status_code_max_2h")))
    .withColumn("log_ts_weekofyear", F.weekofyear("log_ts"))
)

base = (
    cleaned
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "status_code",
        "k",
        "status_code_max_2h",
        "status_code_avg_1h",
        "log_ts_weekofyear",
        (F.col("status_code_avg_1h") + F.col("log_ts_weekofyear")).alias("status_code_avg_1h_plus_log_ts_weekofyear"),
    )
)

with_windows = (
    base
    .filter(F.col("status_code_max_2h").between(286, 396))
    .withColumn("log_ts_weekofyear_dense_rank", F.dense_rank().over(w3))
    .withColumn("k_count_status_code", F.count("status_code").over(w4))
)

scored = (
    with_windows
    .select(
        "id",
        "k",
        F.percent_rank().over(w5).alias("status_code_max_2h_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("status_code_max_2h_percent_rank") >= 0.77, "LARGE").otherwise("SMALL").alias("status_code_max_2h_percent_rank_tier"),
    )
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.sum("status_code_max_2h_percent_rank").alias("total_status_code_max_2h_percent_rank"),
        F.max(F.when(F.col("k") == "region", F.col("status_code_max_2h_percent_rank"))).alias("region_max_status_code_max_2h_percent_rank"),
        F.sum(F.when(F.col("k") == "version", F.col("status_code_max_2h_percent_rank"))).alias("version_total_status_code_max_2h_percent_rank"),
    )
    .filter(F.col("version_total_status_code_max_2h_percent_rank") >= 0)
    .orderBy(F.col("status_code_max_2h_percent_rank_tier").desc())
)
