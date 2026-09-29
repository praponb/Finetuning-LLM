from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id")
w4 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.sum("budget").over(w).alias("cumulative_budget"),
        F.ntile(10).over(w2).alias("budget_bucket"),
        F.lag("budget", 2).over(w3).alias("prev_budget"),
        F.sum("budget").over(w4).alias("running_budget"),
    )
)
