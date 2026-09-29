from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "HR", "Engineering"]))
    .select(
        "id",
        "dept_name",
        "budget",
        F.row_number().over(w).alias("rn"),
        F.row_number().over(w2).alias("rn_2"),
    )
)
