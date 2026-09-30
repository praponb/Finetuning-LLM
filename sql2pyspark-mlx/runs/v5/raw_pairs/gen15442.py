from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget_rounded", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("location").orderBy("budget", "id")
w4 = Window.orderBy("running_budget_rounded", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("location").orderBy("last4_count_budget", "id").rowsBetween(-3, -1)
w_hist_2 = Window.partitionBy("location").orderBy("running_budget_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("location").orderBy("last4_avg_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("location").orderBy(F.col("prev_budget").desc(), "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Berlin"))))
    .withColumn("budget_rounded", F.round("budget", 2))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("NONE")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.email").like("%@outlook.com")), "left_anti")
)

with_windows = (
    not_flagged
    .filter(F.col("dept_name") == "Sales")
    .select(
        "id",
        "location",
        "budget",
        "budget_rounded",
        "dept_name_filled",
        F.sum("budget").over(w).alias("last4_sum_budget"),
        F.avg("budget").over(w).alias("last4_avg_budget"),
        F.count("budget").over(w).alias("last4_count_budget"),
        F.sum("budget_rounded").over(w2).alias("running_budget_rounded"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") < 92000), "left_anti")
)

with_windows_departments = (
    kept
    .withColumn("next_last4_count_budget", F.lead("last4_count_budget", 2).over(w_hist))
    .withColumn("running_budget_rounded_avg_2", F.avg("running_budget_rounded").over(w4))
    .withColumn("prev_budget", F.lag("budget", 1).over(w_hist))
    .withColumn("budget_rounded_avg_3_prior", F.avg("budget_rounded").over(w5))
    .withColumn("running_last4_avg_budget", F.sum("last4_avg_budget").over(w_hist_2))
)

scored = (
    with_windows_departments
    .withColumn("last_known_budget_rounded_avg_3_prior", F.last("budget_rounded_avg_3_prior", ignorenulls=True).over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("prev_last4_count_budget", F.lag("last4_count_budget", 1).over(w_hist))
)

filtered_departments = (
    scored
    .filter(F.col("running_budget_rounded_avg_2").isNull())
    .select(
        "*",
        F.coalesce("last4_avg_budget", F.lit(0)).alias("last4_avg_budget_filled"),
        F.round("last4_avg_budget", 1).alias("last4_avg_budget_rounded"),
    )
)

result = (
    filtered_departments
    .select("id", "location", "budget_rounded", "last4_avg_budget_rounded", "budget")
    .unpivot(
        ids=["id", "location"],
        values=["budget_rounded", "last4_avg_budget_rounded", "budget"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
