from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("budget", "id")
w_ordered = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name", "location")

scored = (
    df_departments
    .select(
        "*",
        F.lead("budget", 1, 0).over(w_hist).alias("next_budget"),
        F.sum("budget").over(w_ordered).alias("cumulative_budget"),
        F.min("budget").over(w3).alias("dept_name_min_budget"),
    )
)

result = (
    scored
    .select("id", "dept_name", "budget", "cumulative_budget", "dept_name_min_budget")
    .orderBy(F.col("dept_name_min_budget").desc())
)
