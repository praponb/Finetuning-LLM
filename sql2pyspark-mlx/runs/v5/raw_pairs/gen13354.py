from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.row_number().over(w).alias("event_seq"),
        F.lag("budget", 2).over(w2).alias("prev_budget"),
        F.avg("budget").over(w3).alias("budget_avg_2"),
    )
)
