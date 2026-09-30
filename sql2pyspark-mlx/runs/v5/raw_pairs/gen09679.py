from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("has_fault", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("status_code_high", "id", "k")
w3 = Window.partitionBy("k").orderBy("has_fault", "id", "k")
w4 = Window.partitionBy("level").orderBy("status_code_high", "id", "k").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("level").orderBy("has_fault", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k").orderBy("has_fault_avg_6", "id", "k").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("level").orderBy("running_status_code_high", "id", "k")
w8 = Window.partitionBy("k", "level").orderBy("has_fault_bucket", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("k").orderBy("next_status_code_high", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("level")
w11 = Window.partitionBy("level").orderBy("level_max_has_fault_avg_6", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "level",
        "k",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

scored = (
    flattened_attrs
    .select(
        "id",
        "level",
        "k",
        "value",
        F.sum("status_code_high").over(w).alias("running_status_code_high"),
        F.lead("status_code_high", 1).over(w2).alias("next_status_code_high"),
        F.ntile(10).over(w3).alias("has_fault_bucket"),
        F.avg("has_fault").over(w4).alias("has_fault_avg_6"),
        F.last("k", ignorenulls=True).over(w5).alias("last_known_k"),
    )
)

metrics = (
    scored
    .filter(F.col("running_status_code_high").isNotNull())
    .select(
        "id",
        "level",
        "k",
        "value",
        "next_status_code_high",
        "has_fault_bucket",
        "has_fault_avg_6",
        F.avg("has_fault_avg_6").over(w6).alias("has_fault_avg_6_avg_5"),
        F.ntile(10).over(w7).alias("running_status_code_high_bucket"),
        F.last("has_fault_bucket", ignorenulls=True).over(w8).alias("last_known_has_fault_bucket"),
        F.last("has_fault_avg_6", ignorenulls=True).over(w9).alias("last_known_has_fault_avg_6"),
        F.max("has_fault_avg_6").over(w10).alias("level_max_has_fault_avg_6"),
    )
)

scored_app_logs = (
    metrics
    .select(
        "id",
        "k",
        "next_status_code_high",
        "running_status_code_high_bucket",
        "level_max_has_fault_avg_6",
        F.count("level_max_has_fault_avg_6").over(w11).alias("running_count_level_max_has_fault_avg_6"),
        F.sum("level_max_has_fault_avg_6").over(w11).alias("running_sum_level_max_has_fault_avg_6"),
        F.avg("level_max_has_fault_avg_6").over(w11).alias("running_avg_level_max_has_fault_avg_6"),
        F.min("level_max_has_fault_avg_6").over(w11).alias("running_min_level_max_has_fault_avg_6"),
    )
)

result = (
    scored_app_logs
    .groupBy(
        "k",
        F.when(F.col("level_max_has_fault_avg_6") >= 1, "HIGH").otherwise("LOW").alias("level_max_has_fault_avg_6_tier"),
    )
    .agg(
        F.sum("level_max_has_fault_avg_6").alias("total_level_max_has_fault_avg_6"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_count"),
        F.size(F.collect_set("k")).alias("num_ks_2"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("k").asc_nulls_first())
)
