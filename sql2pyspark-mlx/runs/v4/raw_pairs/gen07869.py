from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id", "manager_id")

result = (
    df_employees
    .filter(F.col("manager_id") == 27)
    .select(
        "id",
        "hire_date",
        "salary",
        "dept_id",
        "manager_id",
        F.sum("salary").over(w).alias("running_salary"),
        F.avg("salary").over(w2).alias("dept_id_avg_salary"),
    )
    .orderBy(F.col("manager_id").desc())
)
