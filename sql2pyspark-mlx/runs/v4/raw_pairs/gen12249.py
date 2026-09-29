from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("is_singapore").asc_nulls_last(), "id")
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location")
w4 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy("cumulative_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("cumulative_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("dept_name").orderBy(F.col("location_sum_budget").desc())
w8 = Window.partitionBy("location").orderBy("last_known_cumulative_budget", "id").rowsBetween(-5, -1)
w9 = Window.orderBy("cumulative_last_known_cumulative_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w11 = Window.partitionBy("dept_name")

filtered = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "Chiang Mai"]))
    .select(
        "*",
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
        F.when(F.col("location") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("resolved_dept_name"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .withColumn("cumulative_budget", F.sum("budget").over(w2))
    .withColumn("location_sum_budget", F.sum("budget").over(w3))
)

metrics = (
    with_windows
    .select(
        "*",
        F.last("cumulative_budget", ignorenulls=True).over(w4).alias("last_known_cumulative_budget"),
        F.sum("budget").over(w5).alias("running_budget"),
        F.sum("location_sum_budget").over(w2).alias("running_location_sum_budget"),
    )
)

enriched = (
    metrics
    .withColumn("cumulative_last_known_cumulative_budget", F.sum("last_known_cumulative_budget").over(w_running))
    .withColumn("location_sum_budget_percent_rank", F.percent_rank().over(w_hist))
)

enriched_departments = (
    enriched
    .select(
        "*",
        F.avg("location_sum_budget_percent_rank").over(w8).alias("location_sum_budget_percent_rank_avg_5_prior"),
        F.last("location", ignorenulls=True).over(w9).alias("last_known_location"),
        F.dense_rank().over(w10).alias("budget_dense_rank"),
        F.sum("running_location_sum_budget").over(w11).alias("dept_name_sum_running_location_sum_budget"),
    )
)

eligible = (
    enriched_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.job_title").isin(["Engineer", "Designer", "Manager"])), "left_semi")
)

summary = (
    eligible
    .groupBy(
        "dept_name",
        "location",
        F.when(F.col("location_sum_budget_percent_rank") >= 0.81, "PREMIUM").otherwise("STANDARD").alias("location_sum_budget_percent_rank_tier"),
    )
    .agg(
        F.max(F.when(F.col("location") == "New York", F.col("running_budget"))).alias("new_york_max_running_budget"),
        F.percentile_approx("location_sum_budget_percent_rank_avg_5_prior", 0.5).alias("location_sum_budget_percent_rank_avg_5_prior_p50"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    summary
    .select(
        "dept_name",
        "location",
        "location_sum_budget_percent_rank_tier",
        "location_sum_budget_percent_rank_avg_5_prior_p50",
    )
    .orderBy(F.col("location").desc_nulls_first())
)
