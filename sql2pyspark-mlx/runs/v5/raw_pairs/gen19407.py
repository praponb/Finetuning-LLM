from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "HR"]))
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        (F.col("budget") - F.lag("budget", 1).over(w)).alias("budget_change"),
        F.row_number().over(w2).alias("rn"),
    )
)
