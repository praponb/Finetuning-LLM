from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level")
w2 = Window.partitionBy("level").orderBy("level_count_flag_bit2")
w3 = Window.partitionBy("level").orderBy("status_code")
w4 = Window.partitionBy("level").orderBy(F.col("level_sum_status_code_high").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "level",
        "status_code",
        "attr_value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

matched_keys = (
    parsed
    .select("level")
    .exceptAll(
        df_app_logs
        .filter(F.col("latency_ms").between(959, 1414))
        .select("level")
    )
)

reconciled = (
    parsed
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("level_sum_status_code_high", F.sum("status_code_high").over(w_part))
    .withColumn("level_count_flag_bit2", F.count("flag_bit2").over(w_part))
)

cleaned = (
    with_windows
    .filter(F.col("status_code") >= 287)
    .withColumn("level_upper", F.upper("level"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w2).alias("level_count_flag_bit2_cume_dist"),
        F.rank().over(w3).alias("status_code_rank"),
        F.rank().over(w4).alias("level_sum_status_code_high_rank"),
    )
)

result = (
    enriched
    .select(
        "level",
        "status_code",
        "flag_bit2",
        "status_code_high",
        "level_sum_status_code_high",
        "level_count_flag_bit2",
        "status_code_rank",
    )
    .orderBy(F.col("level_count_flag_bit2").desc())
)
