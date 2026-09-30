from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("dept_name").orderBy("budget_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("budget_adjusted_avg_3", "id").rowsBetween(-5, -1)
w4 = Window.orderBy("budget_adjusted_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_sum_budget", "id")
w6 = Window.orderBy("budget_adjusted_avg_3", "id")

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_adjusted", F.round((F.col("budget") * 0.9), 2))
    .withColumn("dept_name_lower", F.lower("dept_name"))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("NONE")))
)

with_windows = (
    base
    .select(
        "id",
        "dept_name_lower",
        F.avg("budget").over(w_hist).alias("running_avg_budget"),
        F.count("budget").over(w_hist).alias("running_count_budget"),
        F.max("budget").over(w_hist).alias("running_max_budget"),
        F.sum("budget").over(w_hist).alias("running_sum_budget"),
        F.avg("budget_adjusted").over(w_grp).alias("budget_adjusted_avg_3"),
    )
)

with_windows_departments = (
    with_windows
    .filter(F.col("running_sum_budget") <= 5574000)
    .select(
        "*",
        F.avg("running_max_budget").over(w3).alias("running_max_budget_avg_5_prior"),
        F.last("budget_adjusted_avg_3", ignorenulls=True).over(w4).alias("last_known_budget_adjusted_avg_3"),
    )
)

enriched = (
    with_windows_departments
    .withColumn("next_budget_adjusted_avg_3", F.lead("budget_adjusted_avg_3").over(w5))
    .withColumn("prev_budget_adjusted_avg_3", F.lag("budget_adjusted_avg_3").over(w5))
    .withColumn("next_running_avg_budget", F.lead("running_avg_budget").over(w5))
    .withColumn("prev_running_avg_budget", F.lag("running_avg_budget").over(w5))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("next_budget_adjusted_avg_3_2", F.lead("budget_adjusted_avg_3", 1).over(w6))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("next_running_avg_budget") >= 3437000, "LARGE").otherwise("SMALL").alias("next_running_avg_budget_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("running_max_budget_avg_5_prior").alias("total_running_max_budget_avg_5_prior"),
    )
    .filter(F.col("total_running_max_budget_avg_5_prior") > 0)
)
