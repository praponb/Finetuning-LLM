from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("status_code_high").asc_nulls_first(), "id", "key")
w2 = Window.partitionBy("key").orderBy("status_code", "id", "key").rowsBetween(-2, -1)
w3 = Window.partitionBy("key").orderBy("has_fault", "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

base = (
    parsed
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.coalesce("status_code_high", F.lit(0)).alias("status_code_high_filled"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_clean"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.avg("status_code_high_filled").over(w2).alias("status_code_high_filled_avg_2_prior"),
        F.lag("status_code", 1, 0).over(w3).alias("prev_status_code"),
    )
)

result = (
    scored
    .select(
        "id",
        "status_code",
        "key",
        "has_fault",
        "status_code_high_filled",
        "key_clean",
        "rn",
        "status_code_high_filled_avg_2_prior",
        "prev_status_code",
        F.when(F.col("key").rlike("^(version|region)$"), "CORE").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy(
        F.col("status_code_high_filled_avg_2_prior").asc_nulls_first(),
        "status_code_high_filled",
    )
)
