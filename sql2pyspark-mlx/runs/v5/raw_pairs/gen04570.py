from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("host").orderBy(F.col("status_code_high").desc())
w3 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("status_code_high").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "host",
        "log_ts",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    parsed
    .filter(F.col("has_fault") == 1)
    .select(
        "id",
        "has_fault",
        "status_code_high",
        F.count("has_fault").over(w).alias("has_fault_count_1h"),
        F.percent_rank().over(w2).alias("status_code_high_percent_rank"),
        F.max("status_code_high").over(w3).alias("status_code_high_max_1h"),
        F.rank().over(w4).alias("status_code_high_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("status_code_high_rank") >= 5)
    .withColumn("status_code_high_plus_has_fault", (F.col("status_code_high") + F.col("has_fault")))
    .withColumn("status_code_high_filled", F.coalesce("status_code_high", F.lit(0)))
)

valid = (
    cleaned
    .filter(F.col("status_code_high") >= 1)
    .select(
        "id",
        "has_fault",
        "status_code_high_rank",
        "status_code_high_filled",
        F.round("status_code_high_filled", 0).alias("status_code_high_filled_rounded"),
    )
)

cleaned_app_logs = (
    valid
    .filter(F.col("status_code_high_filled_rounded") > 40)
    .select(
        "id",
        "has_fault",
        "status_code_high_rank",
        "status_code_high_filled",
        F.when(F.col("status_code_high_filled") >= 85, "High").when(F.col("status_code_high_filled") >= 18, "Medium").otherwise("Low").alias("status_code_high_filled_level"),
    )
)

base = (
    cleaned_app_logs
    .filter(F.col("has_fault") == 0)
    .select(
        "id",
        "status_code_high_rank",
        "status_code_high_filled",
        F.least("status_code_high_filled", "status_code_high_rank").alias("least_status_code_high_filled_status_code_high_rank"),
    )
)

result = base.select("id", "status_code_high_rank", "status_code_high_filled")
