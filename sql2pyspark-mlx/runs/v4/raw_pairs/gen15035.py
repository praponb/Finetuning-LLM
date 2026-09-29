from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name")

result = (
    df_departments
    .filter(F.col("budget") <= 2183000)
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.row_number().over(w).alias("budget_row"),
        F.count("*").over(w2).alias("dept_name_count"),
    )
)
