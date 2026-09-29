from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("status_code").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

latest = (
    lines
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

filtered = (
    latest
    .filter(~F.col("k").eqNullSafe(F.lit("trace")))
    .select(
        "id",
        "host",
        "status_code",
        "k",
        "attr_value",
        F.when(F.col("status_code") >= 442, "High").when(F.col("status_code") >= 264, "Medium").otherwise("Low").alias("status_code_band"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.count("status_code").over(w2).alias("running_count_status_code"),
        F.min("status_code").over(w2).alias("running_min_status_code"),
        F.max("status_code").over(w2).alias("running_max_status_code"),
    )
)

cleaned = (
    enriched
    .filter(F.col("running_count_status_code") <= 7)
    .withColumn("running_max_status_code_whole", F.floor("running_max_status_code"))
    .withColumn("k_filled", F.coalesce("k", F.lit("UNKNOWN")))
)

filtered_app_logs = (
    cleaned
    .filter(F.col("running_min_status_code") > 390)
    .select(
        "id",
        "k",
        "attr_value",
        "running_count_status_code",
        "running_min_status_code",
        "running_max_status_code_whole",
        "k_filled",
        F.trim("host").alias("clean_host"),
    )
)

result = (
    filtered_app_logs
    .select(
        "id",
        "k",
        "attr_value",
        "running_min_status_code",
        "running_max_status_code_whole",
        "k_filled",
    )
)
