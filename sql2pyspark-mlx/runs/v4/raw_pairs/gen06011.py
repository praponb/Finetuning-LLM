from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.orderBy(F.col("flag_bit4").desc_nulls_last())
w3 = Window.orderBy(F.col("status_code_high").desc_nulls_last())
w4 = Window.orderBy("status_code_high")
w5 = Window.partitionBy("k").orderBy(F.col("flag_bit4").desc_nulls_first())
w6 = Window.partitionBy("k").orderBy(F.col("flag_bit4_scaled").desc())
w7 = Window.partitionBy("k").orderBy(F.col("status_code_high").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

unpacked = (
    flattened
    .filter(F.col("k").isNull())
    .select(
        "id",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
    )
)

enriched = (
    unpacked
    .withColumn("k_sum_status_code_high", F.sum("status_code_high").over(w))
    .withColumn("flag_bit4_percent_rank", F.percent_rank().over(w2))
    .withColumn("status_code_high_rank", F.rank().over(w3))
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w4))
    .withColumn("flag_bit4_rank", F.rank().over(w5))
)

base = (
    enriched
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.round((F.col("flag_bit4") * 0.85), 2).alias("flag_bit4_scaled"),
        F.lower("k").alias("k_lower"),
    )
)

enriched_app_logs = (
    base
    .select(
        "*",
        F.rank().over(w6).alias("flag_bit4_scaled_rank"),
        F.dense_rank().over(w7).alias("status_code_high_dense_rank_2"),
        F.sum("flag_bit4").over(w).alias("k_sum_flag_bit4"),
        F.max("status_code_high").over(w).alias("k_max_status_code_high"),
    )
)

filtered = (
    enriched_app_logs
    .filter(F.col("k").isNotNull())
    .withColumn("flag_bit4_percent_rank_per_flag_bit4_scaled", (F.col("flag_bit4_percent_rank") / F.nullif("flag_bit4_scaled", F.lit(0))))
    .withColumn("k_filled", F.coalesce("k", F.lit("UNKNOWN")))
)

daily = (
    filtered
    .groupBy("k")
    .agg(
        F.max("flag_bit4_rank").alias("max_flag_bit4_rank"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_count"),
        F.percentile_approx("k_max_status_code_high", [0.25, 0.5, 0.75]).alias("k_max_status_code_high_quartiles"),
    )
)

result = (
    daily
    .select("k", "max_flag_bit4_rank", "region_count")
    .orderBy(F.col("max_flag_bit4_rank").desc_nulls_first(), F.col("k").desc())
)
