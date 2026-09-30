from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_auth").desc_nulls_last())
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high_num").asc_nulls_last())
w_hist = Window.orderBy(F.col("has_fault").desc_nulls_last())
w4 = Window.partitionBy("attr_key", "service").orderBy(F.col("status_code_high").asc_nulls_first())
w5 = Window.orderBy(F.col("has_fault_rank").asc_nulls_last())
w6 = Window.orderBy(F.col("status_code_high_num_percent_rank").asc_nulls_last())
w7 = Window.partitionBy("attr_key")
w8 = Window.partitionBy("attr_key").orderBy(F.col("has_fault_rank").desc_nulls_last())
w9 = Window.partitionBy("attr_key", "service")
w10 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high_num_percent_rank_percent_rank").desc_nulls_last())
w_ordered = Window.partitionBy("attr_key").orderBy(F.col("status_code_high_cume_dist").desc_nulls_first())
w12 = Window.partitionBy("service").orderBy("attr_key_count_status_code_high_num_percent_rank_percent_rank")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

parsed = (
    flattened
    .filter(F.col("attr_key").isNotNull())
    .select(
        "service",
        "host",
        "attr_key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    parsed
    .filter(F.col("attr_key").isNotNull())
    .select(
        "service",
        "attr_key",
        "value",
        "status_code_high",
        "has_fault",
        F.col("status_code_high").cast("double").alias("status_code_high_num"),
        F.when(F.col("service") == "auth", 1).otherwise(0).alias("is_auth"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.rank().over(w).alias("is_auth_rank"),
        F.percent_rank().over(w2).alias("status_code_high_num_percent_rank"),
        F.dense_rank().over(w_hist).alias("has_fault_dense_rank"),
        F.cume_dist().over(w4).alias("status_code_high_cume_dist"),
        F.rank().over(w_hist).alias("has_fault_rank"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w5).alias("has_fault_rank_percent_rank"),
        F.percent_rank().over(w6).alias("status_code_high_num_percent_rank_percent_rank"),
    )
)

metrics_app_logs = (
    scored
    .select(
        "*",
        F.round((F.col("is_auth_rank") / F.sum("is_auth_rank").over(w7)), 4).alias("is_auth_rank_share"),
        F.count("is_auth_rank").over(w7).alias("attr_key_count_is_auth_rank"),
        F.percent_rank().over(w8).alias("has_fault_rank_percent_rank_2"),
        F.sum("has_fault_rank_percent_rank").over(w9).alias("attr_key_sum_has_fault_rank_percent_rank"),
        F.count("status_code_high_num_percent_rank_percent_rank").over(w7).alias("attr_key_count_status_code_high_num_percent_rank_percent_rank"),
    )
)

enriched = (
    metrics_app_logs
    .withColumn("status_code_high_num_percent_rank_percent_rank_percent_rank", F.percent_rank().over(w10))
    .withColumn("status_code_high_cume_dist_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("attr_key_count_status_code_high_num_percent_rank_percent_rank_dense_rank", F.dense_rank().over(w12))
    .withColumn("attr_key_min_status_code_high_num", F.min("status_code_high_num").over(w7))
)

result = (
    enriched
    .groupBy("attr_key")
    .pivot("service", ["billing", "search"])
    .agg(
        F.max("is_auth_rank").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
