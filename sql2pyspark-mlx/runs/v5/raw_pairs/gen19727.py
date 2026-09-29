from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_recent = Window.partitionBy("attr_key", "host")
w4 = Window.partitionBy("attr_key", "host").orderBy(F.col("status_code").desc(), "id", "attr_key")
w_ordered = Window.partitionBy("host").orderBy("status_code_max_30m", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("attr_key_avg_status_code", "id", "attr_key").rowsBetween(-5, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.first("attr_key", ignorenulls=True).over(w).alias("first_known_attr_key"),
        F.max("status_code").over(w_running).alias("status_code_max_30m"),
        F.avg("status_code").over(w_recent).alias("attr_key_avg_status_code"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.count("status_code").over(w_ordered).alias("running_count_status_code"),
        F.avg("status_code").over(w_ordered).alias("running_avg_status_code"),
        F.max("status_code").over(w_ordered).alias("running_max_status_code"),
        F.sum("status_code").over(w_ordered).alias("running_sum_status_code"),
        F.avg("status_code").over(w6).alias("status_code_avg_5"),
    )
)

result = (
    metrics
    .select(
        "id",
        "status_code",
        "attr_key",
        "attr_value",
        "status_code_max_30m",
        "attr_key_avg_status_code",
        "running_count_status_code",
        "running_sum_status_code",
        "status_code_avg_5",
        F.when(F.col("attr_key").rlike("^(region|version)$"), "GROUP_B").otherwise("PRIORITY").alias("attr_key_zone"),
    )
    .orderBy(F.col("running_sum_status_code").asc_nulls_first())
)
