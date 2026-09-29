from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget_whole", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.floor("budget").alias("budget_whole"),
        F.upper("dept_name").alias("dept_name_upper"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.avg("budget").over(w).alias("budget_avg_2"),
        F.sum("budget").over(w_part).alias("cumulative_budget"),
    )
)
