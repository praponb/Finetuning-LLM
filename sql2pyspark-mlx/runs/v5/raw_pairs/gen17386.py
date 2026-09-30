from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location") != "Singapore")
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.row_number().over(w).alias("row_num"),
        F.row_number().over(w2).alias("budget_row"),
    )
    .orderBy(F.col("row_num").desc())
)
