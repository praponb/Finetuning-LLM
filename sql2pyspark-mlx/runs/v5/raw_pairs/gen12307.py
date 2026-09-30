from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

with_windows = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Finance", "HR"]))
    .select(
        "*",
        F.lead("budget", 1).over(w).alias("next_budget"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = with_windows.select("id", "dept_name", "budget", "next_budget")
