from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name")

result = (
    df_departments
    .select(
        "id",
        "budget",
        "dept_name",
        F.sum("budget").over(w).alias("running_budget"),
        F.count("*").over(w2).alias("dept_name_count"),
    )
    .orderBy(F.col("running_budget").desc())
)
