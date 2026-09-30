from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("dept_name") != "Legal")
    .select(
        "id",
        "dept_name",
        "budget",
        F.round(F.avg("budget").over(w), 2).alias("budget_moving_avg_3"),
        F.lead("budget", 1).over(w2).alias("next_budget"),
    )
)
