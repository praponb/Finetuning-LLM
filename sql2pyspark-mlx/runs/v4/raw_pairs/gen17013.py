from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("latency_ms").desc_nulls_last(), "id", "k")
w_running = Window.partitionBy("k").orderBy("has_fault", "id", "k")
w_grp = Window.partitionBy("k")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

parsed = (
    exploded
    .filter(F.col("latency_ms").between(1167, 1431))
    .select(
        "id",
        "latency_ms",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

ranked = (
    parsed
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "*",
        F.ntile(5).over(w_running).alias("has_fault_bucket"),
        F.corr("status_code", "latency_ms").over(w_grp).alias("corr_status_code_latency_ms"),
        F.min("status_code_high").over(w_grp).alias("k_min_status_code_high"),
        F.ntile(3).over(w_running).alias("has_fault_bucket_2"),
    )
)
