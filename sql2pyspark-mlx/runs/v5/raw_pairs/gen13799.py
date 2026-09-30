from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("level").orderBy(F.col("status_code_high").desc())
w2 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_grp = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_recent = Window.partitionBy("level").orderBy(F.col("has_fault").asc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "log_ts",
        "level",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    flattened_attrs
    .filter((F.col("log_ts") >= F.lit("2025-06-01 14:00:00").cast("timestamp")) & (F.col("status_code_high") > 2))
    .select("*", F.substring("level", 1, 4).alias("level_prefix"))
)

valid_app_logs = (
    valid
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .select(
        "log_ts",
        "level",
        "has_fault",
        "status_code_high",
        "level_prefix",
        F.when(F.col("level") == "WARN", 1).otherwise(0).alias("is_warn"),
    )
)

enriched = (
    valid_app_logs
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("has_fault_sum_2h", F.sum("has_fault").over(w2))
    .withColumn("status_code_high_avg_1h", F.avg("status_code_high").over(w_grp))
    .withColumn("has_fault_cume_dist", F.cume_dist().over(w_recent))
)

valid_app_logs_2 = (
    enriched
    .filter(F.col("log_ts") > F.lit("2025-06-01 10:00:00").cast("timestamp"))
    .select(
        "log_ts",
        "level",
        "status_code_high",
        "level_prefix",
        "is_warn",
        "status_code_high_dense_rank",
        "has_fault_sum_2h",
        "status_code_high_avg_1h",
        F.round((F.col("is_warn") / F.col("status_code_high_dense_rank")), 2).alias("is_warn_per_status_code_high_dense_rank"),
        F.year("log_ts").alias("log_ts_year"),
    )
)

result = (
    valid_app_logs_2
    .groupBy(
        F.when(F.col("status_code_high") >= 2, "PREMIUM").otherwise("STANDARD").alias("status_code_high_tier"),
        F.when(F.col("status_code_high_avg_1h") >= 3, "LARGE").otherwise("SMALL").alias("status_code_high_avg_1h_tier"),
        F.when(F.col("has_fault_sum_2h") >= 4, "LARGE").otherwise("SMALL").alias("has_fault_sum_2h_tier"),
    )
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.round(F.sum("status_code_high_dense_rank") / F.nullif(F.count(F.when(F.col("level") == "DEBUG", 1)), F.lit(0)), 2).alias("status_code_high_dense_rank_per_debug"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
    )
    .orderBy(F.col("status_code_high_tier").desc())
)
