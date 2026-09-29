from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_adjusted").desc())
w2 = Window.orderBy(F.col("status_code_adjusted").desc_nulls_last())
w3 = Window.orderBy("status_code_adjusted")
w4 = Window.orderBy(F.col("status_code").desc())
w5 = Window.orderBy(F.col("is_flagged").desc())
w_part = Window.orderBy(F.col("status_code_high").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "status_code",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    parsed
    .filter(F.col("status_code_high") <= 1)
    .select("*", F.col("status_code_high").cast("string").alias("status_code_high_str"))
)

valid_app_logs = (
    valid
    .filter(F.col("status_code_high") != 2)
    .select(
        "*",
        F.round((F.col("status_code") * 0.85), 2).alias("status_code_adjusted"),
        (F.col("status_code_high") - F.col("status_code")).alias("status_code_high_minus_status_code"),
    )
)

metrics = (
    valid_app_logs
    .withColumn("status_code_adjusted_dense_rank", F.dense_rank().over(w))
    .withColumn("status_code_adjusted_cume_dist", F.cume_dist().over(w2))
    .withColumn("status_code_adjusted_rank", F.rank().over(w3))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w4))
)

metrics_app_logs = (
    metrics
    .withColumn("is_flagged_cume_dist", F.cume_dist().over(w5))
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w_part))
)

result = (
    metrics_app_logs
    .select(
        "status_code",
        "is_flagged",
        "status_code_high",
        "status_code_high_str",
        "status_code_adjusted",
        "status_code_high_minus_status_code",
        "status_code_adjusted_dense_rank",
        "status_code_adjusted_cume_dist",
        "status_code_adjusted_rank",
    )
    .orderBy(F.col("status_code_high").desc(), "status_code_adjusted_dense_rank")
)
