from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

enriched = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.sum("budget").over(w_ordered).alias("running_sum_budget"),
        F.min("budget").over(w_ordered).alias("running_min_budget"),
        F.count("budget").over(w_ordered).alias("running_count_budget"),
        F.lag("budget", 2).over(w2).alias("prev_budget"),
    )
)

result = (
    enriched
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4378000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.percentile_approx("running_sum_budget", [0.25, 0.5, 0.75]).alias("running_sum_budget_quartiles"),
        F.round(F.avg("running_min_budget"), 2).alias("avg_running_min_budget"),
        F.sum(F.when(F.col("location") == "Bangkok", F.col("running_sum_budget")).otherwise(0)).alias("bangkok_running_sum_budget"),
        F.sum(F.when(F.col("location") == "New York", F.col("running_sum_budget")).otherwise(0)).alias("new_york_running_sum_budget"),
        F.sum("running_sum_budget").alias("total_running_sum_budget"),
    )
    .orderBy("budget_tier")
)
