from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k").orderBy("status_code", "id", "k")
w_running = Window.partitionBy("k").orderBy("status_code_high", "id", "k").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("k").orderBy("status_code_high_avg_2", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "status_code",
        "k",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

metrics = (
    parsed
    .select(
        "*",
        F.avg("status_code_high").over(w).alias("status_code_high_avg_2"),
        F.sum("status_code_high").over(w2).alias("k_sum_status_code_high"),
        F.ntile(3).over(w3).alias("status_code_bucket"),
    )
)

enriched = (
    metrics
    .filter(F.col("has_fault").between(0, 1))
    .select(
        "id",
        "k",
        "has_fault",
        "k_sum_status_code_high",
        F.sum("has_fault").over(w_running).alias("last7_sum_has_fault"),
        F.avg("has_fault").over(w_running).alias("last7_avg_has_fault"),
        F.count("has_fault").over(w_running).alias("last7_count_has_fault"),
        F.lag("status_code_bucket", 2).over(w5).alias("prev_status_code_bucket"),
    )
)

result = (
    enriched
    .withColumn("prev_status_code_bucket_tier", F.when(F.col("prev_status_code_bucket") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("last7_avg_has_fault_tier", F.when(F.col("last7_avg_has_fault") >= 0.87, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["k", "prev_status_code_bucket_tier", "last7_avg_has_fault_tier"], ["k", "prev_status_code_bucket_tier"], ["k"]],
        "k",
        "prev_status_code_bucket_tier",
        "last7_avg_has_fault_tier",
    )
    .agg(
        F.round(F.sum("last7_count_has_fault") / F.nullif(F.count(F.when(F.col("k") == "trace", 1)), F.lit(0)), 2).alias("last7_count_has_fault_per_trace"),
        F.sum("last7_count_has_fault").alias("total_last7_count_has_fault"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.percentile_approx("prev_status_code_bucket", [0.25, 0.5, 0.75]).alias("prev_status_code_bucket_quartiles"),
        F.percentile_approx("k_sum_status_code_high", [0.25, 0.5, 0.75]).alias("k_sum_status_code_high_quartiles"),
        F.max("k_sum_status_code_high").alias("max_k_sum_status_code_high"),
        F.grouping("k").alias("grouping_level"),
    )
    .orderBy("last7_avg_has_fault_tier")
)
