from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.lead("budget", 2).over(w).alias("next_budget"),
        F.lead("budget", 1).over(w2).alias("next_budget_2"),
    )
    .orderBy("budget", F.col("next_budget_2").desc())
)
