from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "host").orderBy(F.col("flag_bit8").desc_nulls_last(), "id", "attr_key")
w_ordered = Window.partitionBy("attr_key", "host").orderBy("flag_bit8", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w_ordered_2 = Window.partitionBy("attr_key").orderBy(F.col("flag_bit8").desc(), "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy(F.col("latest_rank").desc(), "id", "attr_key")
w5 = Window.partitionBy("attr_key").orderBy("flag_bit8", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("attr_key").orderBy(F.col("last4_count_flag_bit8").desc(), "id", "attr_key")
w7 = Window.partitionBy("attr_key").orderBy(F.col("last4_count_flag_bit8_bucket").desc_nulls_first(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "host",
        "attr_key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

latest = (
    parsed
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "attr_key",
        "status_code_high",
        "flag_bit8",
        "latest_rank",
        F.count("flag_bit8").over(w_ordered).alias("last4_count_flag_bit8"),
        F.max("flag_bit8").over(w_ordered).alias("last4_max_flag_bit8"),
        F.sum("flag_bit8").over(w_ordered).alias("last4_sum_flag_bit8"),
        F.ntile(5).over(w_ordered_2).alias("flag_bit8_bucket"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "attr_key",
        "status_code_high",
        "latest_rank",
        "last4_max_flag_bit8",
        "last4_sum_flag_bit8",
        F.min("status_code_high").over(w5).alias("last3_min_status_code_high"),
        F.count("status_code_high").over(w5).alias("last3_count_status_code_high"),
        F.avg("status_code_high").over(w5).alias("last3_avg_status_code_high"),
        F.ntile(5).over(w6).alias("last4_count_flag_bit8_bucket"),
    )
)

latest_app_logs = (
    metrics
    .withColumn("latest_rank_2", F.row_number().over(w7))
    .filter(F.col("latest_rank_2") == 1)
)

result = (
    latest_app_logs
    .select(
        "id",
        "attr_key",
        "latest_rank",
        "last4_max_flag_bit8",
        "last4_sum_flag_bit8",
        "last3_count_status_code_high",
        "last3_avg_status_code_high",
        F.when(F.col("attr_key").rlike("^(version|region)$"), "GROUP_B").otherwise("EXTENDED").alias("attr_key_zone"),
    )
    .orderBy(F.col("last3_count_status_code_high").desc())
)
