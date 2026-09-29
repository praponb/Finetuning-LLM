from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

with_windows = (
    df_departments
    .filter(F.col("budget") >= 1518000)
    .select(
        "*",
        F.avg("budget").over(w_ordered).alias("last3_avg_budget"),
        F.max("budget").over(w_ordered).alias("last3_max_budget"),
        F.count("budget").over(w_ordered).alias("last3_count_budget"),
        F.sum("budget").over(w_ordered).alias("last3_sum_budget"),
        F.lag("budget", 2, 0).over(w2).alias("prev_budget"),
    )
)

result = (
    with_windows
    .rollup(
        "location",
        F.when(F.col("last3_avg_budget") >= 3111000, "PREMIUM").otherwise("STANDARD").alias("last3_avg_budget_tier"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_rows"),
        F.percentile_approx("last3_max_budget", 0.95).alias("last3_max_budget_p95"),
        F.percentile_approx("last3_sum_budget", 0.9).alias("last3_sum_budget_p90"),
    )
)
