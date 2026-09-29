from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "hire_date",
        "salary",
        "level",
        F.sum("salary").over(w).alias("running_salary"),
        F.sum("salary").over(w2).alias("running_salary_2"),
    )
)
