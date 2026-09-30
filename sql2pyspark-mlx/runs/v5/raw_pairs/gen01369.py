from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.partitionBy("dept_name")

result = (
    df_departments
    .filter(F.col("budget") >= 1459000)
    .select(
        "id",
        "dept_name",
        "location",
        F.lag("budget", 1).over(w).alias("prev_budget"),
        F.max("budget").over(w2).alias("dept_name_max_budget"),
    )
)
