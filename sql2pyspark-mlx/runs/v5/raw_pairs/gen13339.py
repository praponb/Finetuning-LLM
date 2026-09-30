from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w_recent = Window.partitionBy("dept_name")
w_recent_2 = Window.partitionBy("dept_name").orderBy("budget", "id")
w_part = Window.orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.lag("budget", 1).over(w).alias("prev_budget"),
        F.sum("budget").over(w_recent).alias("dept_name_sum_budget"),
        F.lead("budget", 1).over(w_recent_2).alias("next_budget"),
        F.lag("budget", 1, 0).over(w_recent_2).alias("prev_budget_2"),
        F.ntile(10).over(w_part).alias("budget_bucket"),
    )
)
