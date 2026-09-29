from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")

result = (
    df_departments
    .filter(F.col("dept_name") != "Legal")
    .select(
        "id",
        "dept_name",
        "budget",
        F.dense_rank().over(w).alias("budget_dense_rank"),
    )
)
