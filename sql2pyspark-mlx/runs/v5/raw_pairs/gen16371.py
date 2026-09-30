from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "host",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("region"))))
    .select("*", F.trim("host").alias("clean_host"))
)

base = (
    filtered
    .filter(F.col("status_code_high") <= 1)
    .select(
        "attr_key",
        "attr_value",
        "status_code_high",
        "flag_bit1",
        F.concat("host", F.lit("/"), "attr_key").alias("host_attr_key"),
    )
)

with_windows = base.withColumn("attr_key_max_flag_bit1", F.max("flag_bit1").over(w))

summary = (
    with_windows
    .groupBy("attr_key")
    .agg(
        F.round(F.avg("attr_key_max_flag_bit1"), 2).alias("avg_attr_key_max_flag_bit1"),
        F.min("status_code_high").alias("min_status_code_high"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_count"),
        F.round(F.sum("status_code_high") / F.nullif(F.count(F.when(F.col("attr_key") == "trace", 1)), F.lit(0)), 2).alias("status_code_high_per_trace"),
    )
)

result = (
    summary
    .select(
        "attr_key",
        "min_status_code_high",
        "row_count",
        "region_count",
        "status_code_high_per_trace",
    )
    .orderBy("row_count", F.col("status_code_high_per_trace").desc())
)
