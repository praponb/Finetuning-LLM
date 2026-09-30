from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date")
w2 = Window.partitionBy("dept_id")

result = (
    df_employees
    .filter(F.col("dept_id").isin([8, 18, 34, 37]))
    .select(
        "id",
        "manager_id",
        "hire_date",
        "dept_id",
        "salary",
        F.rank().over(w).alias("rnk"),
        F.sum("salary").over(w2).alias("dept_id_sum_salary"),
    )
    .orderBy("hire_date")
)
