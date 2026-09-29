from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w_part = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("status_code", "id", "attr_key")
w4 = Window.partitionBy("level").orderBy("status_code", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

ranked = (
    exploded
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    ranked
    .select(
        "*",
        F.count("status_code").over(w_part).alias("running_count_status_code"),
        F.avg("status_code").over(w_part).alias("running_avg_status_code"),
        F.sum("status_code").over(w_part).alias("running_sum_status_code"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.lag("running_sum_status_code").over(w3).alias("prev_running_sum_status_code"),
        F.lag("running_avg_status_code").over(w3).alias("prev_running_avg_status_code"),
        F.lead("running_avg_status_code").over(w3).alias("next_running_avg_status_code"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

cleaned = (
    metrics
    .filter(F.col("service") == "search")
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "attr_key",
        "running_avg_status_code",
        "running_sum_status_code",
        "prev_running_avg_status_code",
        F.coalesce("running_avg_status_code", F.lit(0)).alias("running_avg_status_code_filled"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select("id", "host", "level", "attr_key", "running_avg_status_code_filled")
)

result = (
    latest
    .groupBy("host")
    .agg(
        F.percentile_approx("running_avg_status_code_filled", 0.95).alias("running_avg_status_code_filled_p95"),
        F.round(F.sum("running_avg_status_code_filled") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("running_avg_status_code_filled_per_region"),
        F.max("running_avg_status_code_filled").alias("max_running_avg_status_code_filled"),
        F.max("running_avg_status_code_filled").alias("max_running_avg_status_code_filled_2"),
    )
    .filter(F.col("max_running_avg_status_code_filled_2") < 1000000)
)
