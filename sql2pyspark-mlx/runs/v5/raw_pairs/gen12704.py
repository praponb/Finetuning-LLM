from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("flag_bit2", "id", "k").rowsBetween(-3, -1)
w2 = Window.partitionBy("k").orderBy("status_code_high", "id", "k")
w3 = Window.partitionBy("k").orderBy("flag_bit2", "id", "k")
w4 = Window.partitionBy("k").orderBy("flag_bit2", "id", "k").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("k").orderBy("status_code_high", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k").orderBy(F.col("status_code_high").desc_nulls_last(), "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

scored = (
    parsed
    .filter(F.col("flag_bit2") == 0)
    .withColumn("flag_bit2_avg_3_prior", F.avg("flag_bit2").over(w))
    .withColumn("status_code_high_bucket", F.ntile(3).over(w2))
    .withColumn("prev_status_code_high", F.lag("status_code_high", 1).over(w3))
    .withColumn("status_code_high_avg_5", F.avg("status_code_high").over(w4))
    .withColumn("running_status_code_high", F.sum("status_code_high").over(w5))
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .select("id", "k", "flag_bit2_avg_3_prior", "status_code_high_bucket")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("flag_bit2_avg_3_prior") >= 1, "LARGE").otherwise("SMALL").alias("flag_bit2_avg_3_prior_tier"),
    )
    .agg(
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_rows"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("k") == "version", F.col("status_code_high_bucket"))).alias("version_total_status_code_high_bucket"),
        F.round(F.sum("status_code_high_bucket") / F.nullif(F.count(F.when(F.col("k") == "trace", 1)), F.lit(0)), 2).alias("status_code_high_bucket_per_trace"),
    )
)
