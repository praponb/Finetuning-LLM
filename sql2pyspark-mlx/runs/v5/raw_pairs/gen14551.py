from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("status_code_high_whole").asc_nulls_last())
w2 = Window.orderBy(F.col("status_code_high").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

cleaned = (
    parsed
    .filter(F.col("has_fault") < 0)
    .withColumn("status_code_high_whole", F.floor("status_code_high"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "attr_value",
        F.cume_dist().over(w_ordered).alias("status_code_high_whole_cume_dist"),
        F.dense_rank().over(w2).alias("status_code_high_dense_rank"),
    )
)

cleaned_app_logs = (
    with_windows
    .filter(F.col("status_code_high_dense_rank") < 5)
    .withColumn("status_code_high_dense_rank_rounded", F.round("status_code_high_dense_rank", 1))
)

result = (
    cleaned_app_logs
    .select("id", "attr_value", "status_code_high_dense_rank")
    .orderBy(F.col("id").desc_nulls_last())
)
