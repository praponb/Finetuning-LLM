from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")

result = (
    df_departments
    .filter(F.col("location") != "Chiang Mai")
    .select(
        "id",
        "dept_name",
        "budget",
        F.sum("budget").over(w).alias("dept_name_sum_budget"),
    )
    .orderBy("dept_name_sum_budget", F.col("id").desc())
)
