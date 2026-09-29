from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("budget").between(2146000, 3417000))
    .select(
        "id",
        "dept_name",
        "budget",
        F.dense_rank().over(w).alias("budget_dense_rank"),
    )
    .orderBy("budget_dense_rank", F.col("id").desc())
)
