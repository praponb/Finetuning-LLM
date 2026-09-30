from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "level",
        "salary",
        "manager_id",
        "dept_id",
        "hire_date",
        F.avg("salary").over(w).alias("level_avg_salary"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)
