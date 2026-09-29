from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.substring("dept_name", 1, 3).alias("dept_name_prefix"),
        F.round("budget", 2).alias("budget_rounded"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "dept_name_prefix",
        F.avg("budget_rounded").over(w_grp).alias("running_avg_budget_rounded"),
        F.min("budget_rounded").over(w_grp).alias("running_min_budget_rounded"),
        F.sum("budget_rounded").over(w_grp).alias("running_sum_budget_rounded"),
        F.max("budget_rounded").over(w_grp).alias("running_max_budget_rounded"),
    )
)
