from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("has_fault").desc_nulls_last())
w2 = Window.orderBy("has_fault")
w3 = Window.partitionBy("key").orderBy(F.col("has_fault").desc_nulls_first())
w4 = Window.orderBy(F.col("status_code").desc())
w5 = Window.partitionBy("level").orderBy("status_code_dense_rank")
w6 = Window.partitionBy("level", "key").orderBy(F.col("status_code_high").desc_nulls_first())
w7 = Window.orderBy(F.col("status_code_dense_rank").asc_nulls_first())
w8 = Window.orderBy(F.col("status_code_dense_rank").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.col("status_code") != 320)
    .select(
        "id",
        "level",
        "status_code",
        "key",
        "value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    flattened_attrs
    .filter(F.col("status_code_high") == 2)
    .withColumn("has_fault_dense_rank", F.dense_rank().over(w))
    .withColumn("has_fault_cume_dist", F.cume_dist().over(w2))
    .withColumn("has_fault_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w4))
)

filtered = (
    scored
    .filter(F.col("status_code_dense_rank") <= 8)
    .select(
        "level",
        "status_code",
        "key",
        "has_fault",
        "status_code_high",
        "has_fault_dense_rank",
        "has_fault_cume_dist",
        "status_code_dense_rank",
        F.coalesce("key", F.lit("unassigned")).alias("key_filled"),
        F.round((F.col("has_fault_cume_dist") / F.col("has_fault_dense_rank")), 2).alias("has_fault_cume_dist_per_has_fault_dense_rank"),
    )
)

with_windows = (
    filtered
    .select(
        "level",
        "status_code",
        "has_fault",
        "status_code_high",
        "has_fault_cume_dist",
        "status_code_dense_rank",
        "has_fault_cume_dist_per_has_fault_dense_rank",
        F.percent_rank().over(w5).alias("status_code_dense_rank_percent_rank"),
        F.rank().over(w4).alias("status_code_rank"),
        F.dense_rank().over(w6).alias("status_code_high_dense_rank"),
        F.dense_rank().over(w7).alias("status_code_dense_rank_dense_rank"),
        F.dense_rank().over(w8).alias("status_code_dense_rank_dense_rank_2"),
    )
)

report = (
    with_windows
    .select(
        "level",
        "has_fault",
        "has_fault_cume_dist",
        "has_fault_cume_dist_per_has_fault_dense_rank",
        "status_code_dense_rank_percent_rank",
        "status_code_rank",
        "status_code_high_dense_rank",
        "status_code_dense_rank_dense_rank",
        "status_code_dense_rank_dense_rank_2",
    )
    .orderBy(F.col("status_code_dense_rank_dense_rank").desc())
)

result = (
    report
    .select(
        "level",
        "has_fault",
        "has_fault_cume_dist",
        "status_code_rank",
        "status_code_high_dense_rank",
        "status_code_dense_rank_dense_rank",
        "status_code_dense_rank_dense_rank_2",
    )
    .orderBy("status_code_dense_rank_dense_rank_2")
)
