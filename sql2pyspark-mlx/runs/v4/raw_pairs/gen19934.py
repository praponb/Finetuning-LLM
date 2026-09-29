from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location") != "New York")
    .select(
        "id",
        "dept_name",
        "budget",
        F.lead("budget", 1).over(w).alias("next_budget"),
    )
)
