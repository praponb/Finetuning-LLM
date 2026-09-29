from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("k").orderBy(F.col("status_code_cume_dist").desc_nulls_first(), "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

valid = (
    flattened
    .filter(F.col("status_code") == 420)
    .select(
        "id",
        "status_code",
        "k",
        "attr_value",
        F.round("latency_ms", 1).alias("latency_ms_rounded"),
        F.col("status_code").cast("string").alias("status_code_num"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_clean"),
    )
)

scored = (
    valid
    .filter(F.col("latency_ms_rounded").between(60, 74))
    .select(
        "id",
        "status_code",
        "k",
        "latency_ms_rounded",
        "status_code_num",
        "k_clean",
        F.cume_dist().over(w).alias("status_code_cume_dist"),
        F.avg("latency_ms_rounded").over(w2).alias("latency_ms_rounded_avg_2"),
        F.percent_rank().over(w3).alias("status_code_percent_rank"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "status_code",
        "k",
        "latency_ms_rounded",
        "status_code_num",
        "k_clean",
        "status_code_cume_dist",
        "latency_ms_rounded_avg_2",
        "status_code_percent_rank",
    )
)

result = (
    latest
    .select(
        "id",
        "status_code",
        "k",
        "latency_ms_rounded",
        "status_code_num",
        "k_clean",
        "latency_ms_rounded_avg_2",
    )
)
