from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("flag_bit1_plus_status_code_high").asc_nulls_last())
w2 = Window.orderBy(F.col("status_code").desc())
w3 = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w4 = Window.orderBy(F.col("flag_bit1_plus_status_code_high").desc_nulls_last())
w5 = Window.orderBy(F.col("status_code_high_rank").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "latency_ms",
        "status_code",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

base = (
    flattened_attrs
    .filter(F.col("flag_bit1").between(0, 1))
    .select("*", F.round((F.col("flag_bit1") * 100), 2).alias("flag_bit1_adjusted"))
)

cleaned = (
    base
    .filter(F.col("status_code_high") < 1)
    .select(
        "latency_ms",
        "status_code",
        "attr_value",
        "status_code_high",
        "flag_bit1_adjusted",
        (F.col("flag_bit1") + F.col("status_code_high")).alias("flag_bit1_plus_status_code_high"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w).alias("flag_bit1_plus_status_code_high_percent_rank"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
        F.dense_rank().over(w).alias("flag_bit1_plus_status_code_high_dense_rank"),
        F.rank().over(w3).alias("status_code_high_rank"),
    )
)

scored = (
    metrics
    .select(
        "latency_ms",
        "status_code",
        "attr_value",
        "status_code_high",
        "flag_bit1_plus_status_code_high",
        "flag_bit1_plus_status_code_high_dense_rank",
        "status_code_high_rank",
        F.dense_rank().over(w4).alias("flag_bit1_plus_status_code_high_dense_rank_2"),
        F.dense_rank().over(w5).alias("status_code_high_rank_dense_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("flag_bit1_plus_status_code_high") < 58)
    .select(
        "*",
        F.col("status_code_high_rank_dense_rank").cast("double").alias("status_code_high_rank_dense_rank_num"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("status_code_high_rank_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("status_code_high_rank_dense_rank_tier"),
        F.when(F.col("status_code_high_rank") >= 7, "LARGE").otherwise("SMALL").alias("status_code_high_rank_tier"),
        F.when(F.col("flag_bit1_plus_status_code_high") >= 79, "LARGE").otherwise("SMALL").alias("flag_bit1_plus_status_code_high_tier"),
    )
    .agg(
        F.percentile_approx("flag_bit1_plus_status_code_high", [0.25, 0.5, 0.75]).alias("flag_bit1_plus_status_code_high_quartiles"),
        F.count("*").alias("cnt"),
        F.round(F.avg("status_code_high_rank_dense_rank_num"), 2).alias("avg_status_code_high_rank_dense_rank_num"),
    )
    .filter(F.col("avg_status_code_high_rank_dense_rank_num") > 0)
)
