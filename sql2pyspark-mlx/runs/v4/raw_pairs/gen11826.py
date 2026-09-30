from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.ntile(10).over(w).alias("budget_ntile"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
)
