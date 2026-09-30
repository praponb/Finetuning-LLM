from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget_rounded", "id")
w2 = Window.partitionBy("location").orderBy("budget_rounded", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.partitionBy("dept_name").orderBy("budget", "id")
w_recent = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location")

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.concat("location", F.lit(" - "), "dept_name").alias("location_dept_name"),
        F.round("budget", 1).alias("budget_rounded"),
    )
)

result = (
    base
    .filter(F.col("budget") > 2145000)
    .select(
        "id",
        "budget",
        "budget_rounded",
        F.row_number().over(w).alias("row_seq"),
        F.avg("budget_rounded").over(w2).alias("budget_rounded_avg_2"),
        F.ntile(5).over(w_running).alias("budget_bucket"),
        F.last("budget_rounded", ignorenulls=True).over(w_recent).alias("last_known_budget_rounded"),
        F.avg("budget").over(w5).alias("location_avg_budget"),
    )
)
