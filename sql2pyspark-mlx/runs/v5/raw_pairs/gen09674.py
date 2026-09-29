from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")
w_ordered = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("budget", "id")
w4 = Window.partitionBy("location").orderBy("running_sum_latest_rank_plus_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_sum_latest_rank_plus_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("location").orderBy(F.col("avg_running_avg_latest_rank_plus_budget").desc())

deduped = (
    df_departments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    deduped
    .filter(F.col("location").isNotNull() & (F.col("latest_rank") > 1))
    .select(
        "*",
        (F.col("latest_rank") + F.col("budget")).alias("latest_rank_plus_budget"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "location",
        "latest_rank",
        F.max("latest_rank_plus_budget").over(w_ordered).alias("running_max_latest_rank_plus_budget"),
        F.sum("latest_rank_plus_budget").over(w_ordered).alias("running_sum_latest_rank_plus_budget"),
        F.avg("latest_rank_plus_budget").over(w_ordered).alias("running_avg_latest_rank_plus_budget"),
        F.ntile(4).over(w3).alias("budget_bucket"),
    )
)

scored = (
    enriched
    .withColumn("cumulative_running_sum_latest_rank_plus_budget", F.sum("running_sum_latest_rank_plus_budget").over(w4))
    .withColumn("running_budget_bucket", F.sum("budget_bucket").over(w5))
)

base = (
    scored
    .filter(F.col("cumulative_running_sum_latest_rank_plus_budget") < 1639)
    .select(
        "id",
        "location",
        "running_max_latest_rank_plus_budget",
        "running_avg_latest_rank_plus_budget",
        "cumulative_running_sum_latest_rank_plus_budget",
        "running_budget_bucket",
        F.round("cumulative_running_sum_latest_rank_plus_budget", 1).alias("cumulative_running_sum_latest_rank_plus_budget_rounded"),
        F.coalesce("location", F.lit("OTHER")).alias("location_filled"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([14, 15, 31])), "left_anti")
)

result = (
    kept
    .groupBy(
        "location",
        F.when(F.col("cumulative_running_sum_latest_rank_plus_budget") >= 1859, "HIGH").otherwise("LOW").alias("cumulative_running_sum_latest_rank_plus_budget_tier"),
    )
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.avg("running_avg_latest_rank_plus_budget").alias("avg_running_avg_latest_rank_plus_budget"),
        F.percentile_approx("running_max_latest_rank_plus_budget", 0.95).alias("running_max_latest_rank_plus_budget_p95"),
    )
    .withColumn("avg_running_avg_latest_rank_plus_budget_dense_rank", F.dense_rank().over(w6))
)
