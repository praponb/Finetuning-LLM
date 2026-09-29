from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "London", "Singapore"]))
    .select(
        "*",
        F.sum("budget").over(w_ordered).alias("running_budget"),
        F.ntile(3).over(w_grp).alias("budget_bucket"),
    )
)
