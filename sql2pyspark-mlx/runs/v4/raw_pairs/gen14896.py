from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .select(
        "id",
        "location",
        "budget",
        F.lag("budget", 2).over(w).alias("prev_budget"),
        F.lag("budget", 1).over(w).alias("prev_budget_2"),
    )
    .orderBy("prev_budget", F.col("location").desc())
)
