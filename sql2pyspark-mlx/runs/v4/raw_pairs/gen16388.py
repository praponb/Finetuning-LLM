from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_hist = Window.partitionBy("level").orderBy(F.col("status_code").desc())
w3 = Window.orderBy("is_flagged")
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.orderBy(F.col("status_code_high").desc_nulls_last())
w6 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w7 = Window.partitionBy("level")
w8 = Window.partitionBy("status_code_tier").orderBy(F.col("status_code_bit_or").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .filter(F.col("status_code") < 351)
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    parsed
    .withColumn("is_flagged_max_2h", F.max("is_flagged").over(w))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w_hist))
    .withColumn("is_flagged_cume_dist", F.cume_dist().over(w3))
    .withColumn("status_code_high_avg_1h", F.avg("status_code_high").over(w4))
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w5))
)

base = (
    metrics
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .withColumn("status_code_str", F.col("status_code").cast("string"))
)

enriched = (
    base
    .select(
        "*",
        F.sum("status_code_percent_rank").over(w).alias("status_code_percent_rank_sum_2h"),
        F.count("is_flagged_max_2h").over(w6).alias("is_flagged_max_2h_count_15m"),
        F.max("status_code_high").over(w7).alias("level_max_status_code_high"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("status_code") >= 425, "HIGH").otherwise("LOW").alias("status_code_tier"),
        "level",
    )
    .agg(
        F.avg("status_code_percent_rank").alias("avg_status_code_percent_rank"),
        F.min("log_ts").alias("first_seen"),
        F.bit_or("status_code").alias("status_code_bit_or"),
    )
    .withColumn("status_code_bit_or_percent_rank", F.percent_rank().over(w8))
)
