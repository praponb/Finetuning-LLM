from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("attr_key", "service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

with_windows = (
    exploded
    .filter(~F.col("attr_key").isin(["region", "version"]))
    .select(
        "service",
        "status_code",
        "attr_key",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share_2"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share_3"),
    )
)

filtered = (
    with_windows
    .filter(F.col("attr_key").isNotNull() & ~F.col("attr_key").isin(["version", "region"]))
    .select(
        "status_code",
        "attr_key",
        "status_code_share_2",
        "status_code_share_3",
        F.least("status_code_share_3", "status_code_share_2").alias("least_status_code_share_3_status_code_share_2"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("attr_key").isNotNull() & F.col("least_status_code_share_3_status_code_share_2").isNull())
    .select(
        "attr_key",
        F.col("status_code_share_3").cast("int").alias("status_code_share_3_int"),
    )
)

valid = (
    filtered_app_logs
    .filter(F.col("attr_key") == "version")
    .withColumn("status_code_share_3_int_num", F.col("status_code_share_3_int").cast("string"))
    .withColumn("status_code_share_3_int_num_2", F.col("status_code_share_3_int").cast("double"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("DIRECT")))
)

cleaned = (
    valid
    .filter(F.col("attr_key").isNotNull())
    .select(
        "attr_key",
        "status_code_share_3_int",
        "status_code_share_3_int_num",
        "status_code_share_3_int_num_2",
        F.greatest("status_code_share_3_int_num_2", "status_code_share_3_int").alias("greatest_status_code_share_3_int_num_2_status_code_share_3_int"),
        F.ceil("status_code_share_3_int").alias("status_code_share_3_int_whole"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("greatest_status_code_share_3_int_num_2_status_code_share_3_int") >= 72, "HIGH").otherwise("LOW").alias("greatest_status_code_share_3_int_num_2_status_code_share_3_int_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum("status_code_share_3_int_whole") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("status_code_share_3_int_whole_per_region"),
        F.round(F.sum("greatest_status_code_share_3_int_num_2_status_code_share_3_int") / F.nullif(F.count(F.when(F.col("attr_key") == "trace", 1)), F.lit(0)), 2).alias("greatest_status_code_share_3_int_num_2_status_code_share_3_int_per_trace"),
    )
    .orderBy("num_rows")
)
