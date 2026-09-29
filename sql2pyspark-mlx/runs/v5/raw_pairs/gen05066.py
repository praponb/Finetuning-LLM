from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy(F.col("flag_bit8_count_2h").desc_nulls_first())
w4 = Window.orderBy(F.col("level_max_status_code_high").asc_nulls_last())
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("level_max_status_code_high_tier").orderBy(F.col("status_code_bit_or").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    parsed
    .select(
        "log_ts",
        "status_code",
        "status_code_high",
        F.max("status_code_high").over(w).alias("level_max_status_code_high"),
        F.count("flag_bit8").over(w2).alias("flag_bit8_count_2h"),
    )
)

scored = (
    metrics
    .select(
        "status_code",
        "level_max_status_code_high",
        F.rank().over(w3).alias("flag_bit8_count_2h_rank"),
        F.percent_rank().over(w4).alias("level_max_status_code_high_percent_rank"),
        F.count("status_code").over(w5).alias("status_code_count_15m"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("level_max_status_code_high") >= 3, "PREMIUM").otherwise("STANDARD").alias("level_max_status_code_high_tier"),
        F.when(F.col("level_max_status_code_high_percent_rank") >= 0.88, "HIGH").otherwise("LOW").alias("level_max_status_code_high_percent_rank_tier"),
    )
    .agg(
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.bit_or("status_code").alias("status_code_bit_or"),
    )
    .filter(F.col("status_code_bit_and") >= 1)
    .withColumn("status_code_bit_or_rank", F.rank().over(w6))
    .orderBy(F.col("status_code_bit_or").desc_nulls_first())
)
