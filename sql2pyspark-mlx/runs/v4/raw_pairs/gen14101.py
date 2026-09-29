from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "host")
w2 = Window.orderBy("status_code")
w3 = Window.partitionBy("host").orderBy("status_code")
w4 = Window.partitionBy("attr_key")
w5 = Window.orderBy(F.col("attr_key_max_status_code").desc())
w6 = Window.partitionBy("attr_key", "level")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

kept_keys = (
    lines
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.to_date("log_ts") >= "2023-11-01")
        .select("id")
    )
)

reconciled = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.max("status_code").over(w).alias("attr_key_max_status_code"),
        F.dense_rank().over(w2).alias("status_code_dense_rank"),
        F.cume_dist().over(w3).alias("status_code_cume_dist"),
    )
)

with_windows = (
    metrics
    .withColumn("attr_key_sum_status_code_cume_dist", F.sum("status_code_cume_dist").over(w4))
    .withColumn("status_code_dense_rank_2", F.dense_rank().over(w2))
    .withColumn("attr_key_max_status_code_percent_rank", F.percent_rank().over(w5))
    .withColumn("attr_key_avg_attr_key_max_status_code", F.avg("attr_key_max_status_code").over(w6))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("status_code_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("status_code_dense_rank_tier"),
        "level",
    )
    .agg(
        F.count_if(F.col("level") == "DEBUG").alias("debug_events"),
        F.max("attr_key_max_status_code").alias("max_attr_key_max_status_code"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.avg("attr_key_max_status_code").alias("avg_attr_key_max_status_code"),
        F.sum(F.when(F.col("level") == "WARN", F.col("status_code_cume_dist")).otherwise(0)).alias("warn_status_code_cume_dist"),
    )
    .filter(F.col("debug_events") >= 1)
    .orderBy(F.col("warn_status_code_cume_dist").desc())
)
